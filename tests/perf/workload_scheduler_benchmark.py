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

"""Compare end-to-end GreptimeDB performance with the scheduler off and on.

Consumes only normalized JSON from ``query_perf_fixture plan --case``. Every
sample starts a fresh standalone server and seeds an equivalent Mito table.
Modes are interleaved by ``(iteration_index + phase_index) % 2`` to reduce
time/order bias.

Usage:

    uv run --no-project tests/perf/workload_scheduler_benchmark.py \\
        --fixture-generator /path/to/query_perf_fixture \\
        --binary /path/to/greptime \\
        --work-dir /tmp/ws-bench \\
        [--case tests/perf/query_cases/workload_scheduler_2_8/case.toml] \\
        [--dry-run] [--no-gate] [--reuse-work-dir]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import workload_scheduler_runner as workload
import workload_scheduler_report as ws_report


BUILTIN_CASE = (
    Path(__file__).resolve().parent
    / "query_cases"
    / "workload_scheduler_2_8"
    / "case.toml"
)
REQUIRED_PHASES = ("query_only", "write_only", "light_write", "saturated")


def _resolve_binary(bin_path: Path) -> Path:
    resolved = bin_path.resolve()
    return resolved


def _check_binary(bin_path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    resolved = _resolve_binary(bin_path)
    if not resolved.is_file():
        raise FileNotFoundError(f"binary does not exist: {resolved}")
    if not os.access(str(resolved), os.X_OK):
        raise PermissionError(f"binary is not executable: {resolved}")


def reserve_ports(count: int) -> list[int]:
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [int(sock.getsockname()[1]) for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def write_config(
    path: Path,
    enabled: bool,
    ports: list[int],
    runtime_size: int,
    max_concurrent_polls: int,
    compact_size: int = 1,
    query_size: int = 4,
    ingest_size: int = 4,
    query_weight: int = 2,
    write_weight: int = 8,
) -> None:
    path.write_text(
        f"""\
[runtime]
global_rt_size = {runtime_size}
compact_rt_size = {compact_size}
query_rt_size = {query_size}
ingest_rt_size = {ingest_size}

[runtime.experimental_workload_scheduler]
enable = {str(enabled).lower()}
max_concurrent_polls = {max_concurrent_polls}
query_weight = {query_weight}
write_weight = {write_weight}

[http]
addr = "127.0.0.1:{ports[0]}"

[grpc]
bind_addr = "127.0.0.1:{ports[1]}"

[mysql]
enable = false
addr = "127.0.0.1:{ports[2]}"

[postgres]
enable = false
addr = "127.0.0.1:{ports[3]}"
"""
    )


def wait_for_server(
    client: workload.SqlClient,
    process: subprocess.Popen[bytes],
    log_path: Path,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    last_error: Any = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = log_path.read_text(errors="replace")[-8_000:]
            raise RuntimeError(
                f"GreptimeDB exited with {process.returncode} during startup:\n{tail}"
            )
        ok, _, last_error = client.sql("SELECT 1")
        if ok:
            return
        time.sleep(0.2)
    raise TimeoutError(f"GreptimeDB did not become ready: {last_error}")


def stop_server(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


# ---------------------------------------------------------------------------
# Planner integration
# ---------------------------------------------------------------------------


def load_plan(
    fixture_generator: Path,
    case_path: Path,
) -> dict[str, Any]:
    """Invoke the Rust planner and return the normalized scenario JSON."""
    cmd = [str(fixture_generator.resolve()), "plan", "--case", str(case_path)]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"query_perf_fixture plan failed (exit {result.returncode}):\n"
            f"{result.stderr[:2000]}"
        )
    try:
        plan = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"plan output is not valid JSON: {e}") from e
    return plan


def _check_exact_int(val: object, path: str) -> int:
    """Require val to be exact int (bool and all floats rejected)."""
    if isinstance(val, bool):
        raise ValueError(f"{path} must be int, got bool")
    if not isinstance(val, int):
        raise ValueError(f"{path} must be int, got {type(val).__name__}: {val!r}")
    return int(val)


def _check_exact_positive_int(val: object, path: str) -> int:
    """Require val to be exact positive int (bool rejected)."""
    iv = _check_exact_int(val, path)
    if iv <= 0:
        raise ValueError(f"{path} must be positive, got {iv}")
    return iv


def _check_exact_nonnegative_int(val: object, path: str) -> int:
    """Require val to be exact nonnegative int (bool rejected)."""
    iv = _check_exact_int(val, path)
    if iv < 0:
        raise ValueError(f"{path} must be nonnegative, got {iv}")
    return iv


def _check_finite_number(val: object, path: str) -> float:
    """Require val to be finite numeric (bool rejected)."""
    if isinstance(val, bool):
        raise ValueError(f"{path} must be numeric, got bool")
    if not isinstance(val, (int, float)):
        raise ValueError(f"{path} must be numeric, got {type(val).__name__}: {val!r}")
    if not math.isfinite(val):
        raise ValueError(f"{path} must be finite, got {val}")
    return float(val)


def _check_nonnegative_number(val: object, path: str) -> float:
    """Require val to be finite nonnegative numeric (bool rejected)."""
    fv = _check_finite_number(val, path)
    if fv < 0:
        raise ValueError(f"{path} must be nonnegative, got {fv}")
    return fv


def _check_string(val: object, path: str) -> str:
    """Require val to be a string."""
    if not isinstance(val, str):
        raise ValueError(f"{path} must be str, got {type(val).__name__}: {val!r}")
    return val


def _check_bool(val: object, path: str) -> bool:
    """Require val to be bool (int/float rejected)."""
    if not isinstance(val, bool):
        raise ValueError(f"{path} must be bool, got {type(val).__name__}: {val!r}")
    return val


def validate_scenario_kind(plan: dict[str, Any]) -> str:
    """Extract scenario kind and validate it's ``workload_scheduler``.

    Also validates ``schema_version == 1`` (exact int, bool rejected).
    """
    # schema_version must be exact int 1
    sv = plan.get("schema_version")
    if sv is None:
        raise ValueError("plan.schema_version is missing")
    if isinstance(sv, bool):
        raise ValueError(f"plan.schema_version must be int, got bool: {sv!r}")
    if not isinstance(sv, int):
        raise ValueError(f"plan.schema_version must be int, got {type(sv).__name__}: {sv!r}")
    if sv != 1:
        raise ValueError(f"plan.schema_version must be 1, got {sv!r}")

    scenario = plan.get("scenario", {})
    if isinstance(scenario, dict):
        kind = scenario.get("kind", "")
    else:
        kind = ""
    if kind != "workload_scheduler":
        raise ValueError(
            f"unsupported scenario kind {kind!r}; "
            "expected 'workload_scheduler'"
        )
    return kind


def validate_normalized_scenario(plan: dict[str, Any]) -> dict[str, Any]:
    """Strictly validate every required field in the normalized scenario JSON.

    The Rust planner emits a complete schema; missing/wrong-type fields are
    fatal invalid-plan errors. Every field documented in the workload_scheduler
    case.toml is required. Returns the scenario dict on success; raises
    ValueError with details on first missing/wrong-type field.
    """
    import math
    import sys

    scenario = plan.get("scenario")
    if not isinstance(scenario, dict):
        raise ValueError("plan.scenario is missing or not a dict")

    # Top-level fields with strict type checking
    _check_string(scenario.get("database"), "plan.scenario.database")
    _check_exact_positive_int(scenario.get("iterations"), "plan.scenario.iterations")
    warmup_s = _check_exact_nonnegative_int(scenario.get("warmup_seconds"), "plan.scenario.warmup_seconds")
    duration_s = _check_exact_positive_int(scenario.get("duration_seconds"), "plan.scenario.duration_seconds")
    drain_s = _check_exact_positive_int(scenario.get("drain_timeout_seconds"), "plan.scenario.drain_timeout_seconds")
    if duration_s <= 0:
        raise ValueError(f"plan.scenario.duration_seconds must be positive, got {duration_s}")

    # scrape_interval_seconds must be exactly 1.0
    si = _check_finite_number(scenario.get("scrape_interval_seconds"), "plan.scenario.scrape_interval_seconds")
    if abs(si - 1.0) > sys.float_info.epsilon:
        raise ValueError(f"plan.scenario.scrape_interval_seconds must be 1.0, got {si!r}")

    # expected_scrape_count: exact positive int, consistent with duration/interval + 1
    esc = _check_exact_positive_int(scenario.get("expected_scrape_count"), "plan.scenario.expected_scrape_count")
    expected = int(duration_s / si) + 1  # number of steps at interval 1.0, plus offset 0
    if esc != expected:
        raise ValueError(
            f"plan.scenario.expected_scrape_count ({esc}) is inconsistent with "
            f"duration_seconds ({duration_s}) / scrape_interval_seconds ({si}) + 1 = {expected}"
        )

    # runtime section
    runtime = scenario.get("runtime")
    if not isinstance(runtime, dict):
        raise ValueError("plan.scenario.runtime is missing or not a dict")
    for key in ("global", "compact", "query", "ingest"):
        _check_exact_positive_int(runtime.get(key), f"plan.scenario.runtime.{key}")

    # scheduler section — query_weight must be 2, write_weight must be 8 (fixed 2:8)
    scheduler = scenario.get("scheduler")
    if not isinstance(scheduler, dict):
        raise ValueError("plan.scenario.scheduler is missing or not a dict")
    _check_exact_positive_int(scheduler.get("max_concurrent_polls"), "plan.scenario.scheduler.max_concurrent_polls")
    qw = _check_exact_positive_int(scheduler.get("query_weight"), "plan.scenario.scheduler.query_weight")
    ww = _check_exact_positive_int(scheduler.get("write_weight"), "plan.scenario.scheduler.write_weight")
    if qw != 2:
        raise ValueError(f"plan.scenario.scheduler.query_weight must be 2 (fixed 2:8), got {qw}")
    if ww != 8:
        raise ValueError(f"plan.scenario.scheduler.write_weight must be 8 (fixed 2:8), got {ww}")

    # targets — exactly 2, canonical names "baseline"/"scheduled", booleans locked
    targets = scenario.get("targets")
    if not isinstance(targets, list):
        raise ValueError("plan.scenario.targets must be a list")
    if len(targets) != 2:
        raise ValueError(f"plan.scenario.targets must have exactly 2 entries, got {len(targets)}")
    target_names: set[str] = set()
    enabled_true_count = 0
    enabled_false_count = 0
    for i, t in enumerate(targets):
        if not isinstance(t, dict):
            raise ValueError(f"plan.scenario.targets[{i}] is not a dict")
        name = _check_string(t.get("name"), f"plan.scenario.targets[{i}].name")
        enabled = _check_bool(t.get("scheduler_enabled"), f"plan.scenario.targets[{i}].scheduler_enabled")
        if name in target_names:
            raise ValueError(f"plan.scenario.targets[{i}].name \"{name}\" is duplicated")
        target_names.add(name)
        if enabled:
            enabled_true_count += 1
        else:
            enabled_false_count += 1
        # Exactly mirror Rust target validation:
        if name == "baseline" and enabled:
            raise ValueError("target \"baseline\" must have scheduler_enabled=false")
        if name == "scheduled" and not enabled:
            raise ValueError("target \"scheduled\" must have scheduler_enabled=true")
        if name not in ("baseline", "scheduled"):
            raise ValueError(f"unrecognized target name \"{name}\"; only \"baseline\" and \"scheduled\" are allowed")
    if enabled_true_count != 1:
        raise ValueError(f"must have exactly 1 target with scheduler_enabled=true, found {enabled_true_count}")
    if enabled_false_count != 1:
        raise ValueError(f"must have exactly 1 target with scheduler_enabled=false, found {enabled_false_count}")

    # data section - all exact positive ints
    data = scenario.get("data")
    if not isinstance(data, dict):
        raise ValueError("plan.scenario.data is missing or not a dict")
    for key in ("shards", "seed_rows", "seed_batch_size", "seed_timestamp_millis", "write_sequence_start_millis"):
        _check_exact_positive_int(data.get(key), f"plan.scenario.data.{key}")

    # tables section
    tables = scenario.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("plan.scenario.tables is missing or not a dict")
    for role in ("query", "write"):
        tbl = tables.get(role)
        if not isinstance(tbl, dict):
            raise ValueError(f"plan.scenario.tables.{role} is missing or not a dict")
        _check_string(tbl.get("name"), f"plan.scenario.tables.{role}.name")
        _check_exact_positive_int(tbl.get("partitions"), f"plan.scenario.tables.{role}.partitions")

    # query section
    query = scenario.get("query")
    if not isinstance(query, dict):
        raise ValueError("plan.scenario.query is missing or not a dict")
    qsql = query.get("sql")
    if not isinstance(qsql, str) or not qsql.strip():
        raise ValueError(f"plan.scenario.query.sql must be nonempty string, got {qsql!r}")

    # write section
    write = scenario.get("write")
    if not isinstance(write, dict):
        raise ValueError("plan.scenario.write is missing or not a dict")
    _check_exact_positive_int(write.get("batch_size"), "plan.scenario.write.batch_size")

    # phases - must be a list of exactly 4 phases
    phases = scenario.get("phases")
    if not isinstance(phases, list) or len(phases) != 4:
        raise ValueError("plan.scenario.phases must be a list of exactly 4 entries")
    for i, p in enumerate(phases):
        if not isinstance(p, dict):
            raise ValueError(f"plan.scenario.phases[{i}] is not a dict")
        _check_string(p.get("name"), f"plan.scenario.phases[{i}].name")
        _check_exact_nonnegative_int(p.get("query_workers"), f"plan.scenario.phases[{i}].query_workers")
        _check_exact_nonnegative_int(p.get("write_workers"), f"plan.scenario.phases[{i}].write_workers")
        _check_nonnegative_number(p.get("write_delay_seconds"), f"plan.scenario.phases[{i}].write_delay_seconds")

    # gates section
    gates = scenario.get("gates")
    if not isinstance(gates, dict):
        raise ValueError("plan.scenario.gates is missing or not a dict")
    for key in (
        "max_failure_rate", "max_outstanding_requests",
        "dual_backlog_lower", "dual_backlog_upper",
        "min_dual_backlog_interval_fraction", "min_dual_backlog_polls_per_class",
        "min_single_class_active_purity", "min_light_write_query_share",
        "active_within_scheduler_limit", "max_capacity_normalized_regression_pct",
    ):
        if key not in gates:
            raise ValueError(f"plan.scenario.gates.{key} is missing")

    # Validate individual gate fields with full Rust PerformanceGate range constraints
    # max_failure_rate finite in [0, 1]
    mfr = _check_finite_number(gates["max_failure_rate"], "plan.scenario.gates.max_failure_rate")
    if not 0.0 <= mfr <= 1.0:
        raise ValueError(f"plan.scenario.gates.max_failure_rate must be in [0, 1], got {mfr}")

    _check_exact_nonnegative_int(gates["max_outstanding_requests"], "plan.scenario.gates.max_outstanding_requests")

    # dual_backlog_lower finite in [0, 1]
    dbl = _check_finite_number(gates["dual_backlog_lower"], "plan.scenario.gates.dual_backlog_lower")
    if not 0.0 <= dbl <= 1.0:
        raise ValueError(f"plan.scenario.gates.dual_backlog_lower must be in [0, 1], got {dbl}")

    # dual_backlog_upper finite in [0, 1]
    dbu = _check_finite_number(gates["dual_backlog_upper"], "plan.scenario.gates.dual_backlog_upper")
    if not 0.0 <= dbu <= 1.0:
        raise ValueError(f"plan.scenario.gates.dual_backlog_upper must be in [0, 1], got {dbu}")

    # lower <= upper
    if dbl > dbu:
        raise ValueError(
            f"plan.scenario.gates.dual_backlog_lower ({dbl}) must be "
            f"<= dual_backlog_upper ({dbu})"
        )

    # Must contain derived expected_write_share (0.8)
    expected_write_share = 0.8
    if dbl > expected_write_share or dbu < expected_write_share:
        raise ValueError(
            f"plan.scenario.gates dual_backlog bounds [{dbl}, {dbu}] must contain "
            f"derived expected_write_share {expected_write_share}"
        )

    # min_dual_backlog_interval_fraction finite in [0, 1]
    mdbif = _check_finite_number(gates["min_dual_backlog_interval_fraction"],
                                  "plan.scenario.gates.min_dual_backlog_interval_fraction")
    if not 0.0 <= mdbif <= 1.0:
        raise ValueError(
            f"plan.scenario.gates.min_dual_backlog_interval_fraction must be in [0, 1], got {mdbif}"
        )

    # min_dual_backlog_polls_per_class exact positive int (> 0), not just nonnegative
    _check_exact_positive_int(gates["min_dual_backlog_polls_per_class"],
                               "plan.scenario.gates.min_dual_backlog_polls_per_class")

    # min_single_class_active_purity finite in [0, 1]
    mscap = _check_finite_number(gates["min_single_class_active_purity"],
                                  "plan.scenario.gates.min_single_class_active_purity")
    if not 0.0 <= mscap <= 1.0:
        raise ValueError(
            f"plan.scenario.gates.min_single_class_active_purity must be in [0, 1], got {mscap}"
        )

    # min_light_write_query_share finite in [0, 1]
    mlwqs = _check_finite_number(gates["min_light_write_query_share"],
                                  "plan.scenario.gates.min_light_write_query_share")
    if not 0.0 <= mlwqs <= 1.0:
        raise ValueError(
            f"plan.scenario.gates.min_light_write_query_share must be in [0, 1], got {mlwqs}"
        )

    _check_bool(gates["active_within_scheduler_limit"], "plan.scenario.gates.active_within_scheduler_limit")

    # max_capacity_normalized_regression_pct finite nonnegative
    mcnr = _check_finite_number(gates["max_capacity_normalized_regression_pct"],
                                 "plan.scenario.gates.max_capacity_normalized_regression_pct")
    if mcnr < 0.0:
        raise ValueError(
            f"plan.scenario.gates.max_capacity_normalized_regression_pct must be nonnegative, got {mcnr}"
        )

    return scenario


def extract_phases(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract phases from the normalized plan. Assumes validate_normalized_scenario passed."""
    scenario = plan.get("scenario", {})
    if isinstance(scenario, dict):
        phases = scenario.get("phases", [])
    else:
        phases = []
    return list(phases)


def extract_targets(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract targets from the normalized plan. Assumes validate_normalized_scenario passed."""
    scenario = plan.get("scenario", {})
    if isinstance(scenario, dict):
        targets = scenario.get("targets", [])
    else:
        targets = []
    return list(targets)


def extract_config(plan: dict[str, Any]) -> dict[str, Any]:
    """Extract the full config from the normalized plan for execution. Assumes validate_normalized_scenario passed."""
    scenario = plan.get("scenario", {})
    if not isinstance(scenario, dict):
        return {}
    return dict(scenario)


# ---------------------------------------------------------------------------
# Shared deterministic matrix helper
# ---------------------------------------------------------------------------


def sample_order(iteration_index: int, phase_index: int) -> tuple[bool, ...]:
    """Return (baseline_enabled, scheduled_enabled) order for one sample pair.

    Zero-based (iteration_index + phase_index) % 2:
      even -> baseline (False) then scheduled (True)
      odd  -> scheduled (True) then baseline (False)
    """
    if (iteration_index + phase_index) % 2 == 0:
        return (False, True)
    else:
        return (True, False)


def build_matrix(iterations: int, phases: list[dict[str, Any]], targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build deterministic execution matrix matching dry-run output.

    Uses target ``name`` and ``scheduler_enabled`` from normalized targets.
    Targets must contain exactly 2 entries (baseline=False, scheduled=True)
    as validated by validate_normalized_scenario. The returned list is the
    single source of truth for BOTH dry-run display and execution iteration.
    """
    norm_targets: list[dict[str, Any]] = []
    for t in targets:
        norm_targets.append({
            "name": t["name"],
            "scheduler_enabled": t["scheduler_enabled"],
        })
    if not norm_targets:
        raise ValueError("build_matrix requires at least one target; validate_normalized_scenario guarantees 2")

    matrix = []
    for i in range(iterations):
        for idx, phase in enumerate(phases):
            order = sample_order(i, idx)
            entries = []
            for enabled in order:
                # Find the matching normalized target
                matched = [t for t in norm_targets if t["scheduler_enabled"] == enabled]
                if not matched:
                    raise ValueError(f"build_matrix: no target with scheduler_enabled={enabled}")
                entries.append({
                    "name": matched[0]["name"],
                    "scheduler_enabled": enabled,
                })
            matrix.append({
                "iteration": i + 1,
                "phase": phase["name"],
                "entries": entries,
            })
    return matrix


# ---------------------------------------------------------------------------
# Sample runner
# ---------------------------------------------------------------------------


def run_sample(
    binary: Path,
    root: Path,
    phase_cfg: dict[str, Any],
    enabled: bool,
    target_name: str,
    iteration: int,
    sample_number: int,
    config_overlay: dict[str, Any],
    timeout: float,
    start_timeout: float,
) -> dict[str, Any]:
    """Run one A/B sample for a single phase.

    *target_name* is the normalized target name from the matrix (e.g.
    "baseline" or "scheduled") and is persisted as the definitive
    mode/target field. *enabled* is the boolean scheduler_enabled flag.

    Returns a dict with ``name``, ``mode``, ``sample``, ``requests``, and
    scheduler scrape data.
    """
    mode = target_name
    phase_name = phase_cfg.get("name", "unknown")
    sample_root = root / f"iteration-{iteration:02d}" / phase_name / mode
    data_home = sample_root / "data"
    log_dir = sample_root / "logs"
    config_path = sample_root / "config.toml"
    process_log_path = sample_root / "process.log"
    sample_root.mkdir(parents=True)
    data_home.mkdir()
    log_dir.mkdir()
    ports = reserve_ports(4)

    # All config_overlay values are guaranteed present by validate_normalized_scenario
    runtime_raw = config_overlay["runtime"]
    scheduler_raw = config_overlay["scheduler"]
    runtime_size = int(runtime_raw["global"])
    max_concurrent_polls = int(scheduler_raw["max_concurrent_polls"])
    compact_size = int(runtime_raw["compact"])
    query_size = int(runtime_raw["query"])
    ingest_size = int(runtime_raw["ingest"])
    query_weight = int(scheduler_raw["query_weight"])
    write_weight = int(scheduler_raw["write_weight"])

    write_config(
        config_path,
        enabled,
        ports,
        runtime_size,
        max_concurrent_polls,
        compact_size,
        query_size,
        ingest_size,
        query_weight,
        write_weight,
    )

    environment = os.environ.copy()
    for variable in (
        "ALL_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "all_proxy",
        "https_proxy",
        "http_proxy",
    ):
        environment.pop(variable, None)

    command = [
        str(binary.resolve()),
        "standalone",
        "start",
        "--config-file",
        str(config_path),
        "--data-home",
        str(data_home),
        "--log-dir",
        str(log_dir),
        "--log-level",
        "warn",
    ]

    data_cfg = config_overlay["data"]
    seed_rows = int(data_cfg["seed_rows"])
    seed_batch_size = int(data_cfg["seed_batch_size"])
    warmup_seconds = float(config_overlay["warmup_seconds"])
    duration_seconds = float(config_overlay["duration_seconds"])
    drain_timeout_seconds = float(config_overlay["drain_timeout_seconds"])
    # scrape_interval_seconds is validated by validate_normalized_scenario
    scrape_interval = float(config_overlay["scrape_interval_seconds"])
    write_batch_size = int(config_overlay["write"]["batch_size"])
    database = str(config_overlay["database"])

    # Normalized table/query/write values from scenario plan
    tables_config = config_overlay["tables"]
    query_table_name = str(tables_config["query"]["name"])
    write_table_name = str(tables_config["write"]["name"])
    query_partitions = int(tables_config["query"]["partitions"])
    write_partitions = int(tables_config["write"]["partitions"])
    shards = int(data_cfg["shards"])
    seed_timestamp_millis = int(data_cfg["seed_timestamp_millis"])
    write_sequence_start_millis = int(data_cfg["write_sequence_start_millis"])
    query_sql = str(config_overlay["query"]["sql"])

    # Construct the query SQL with the actual table name substituted
    query_sql_param = query_sql

    with process_log_path.open("wb") as process_log:
        process = subprocess.Popen(
            command,
            stdout=process_log,
            stderr=subprocess.STDOUT,
            env=environment,
        )
        try:
            client = workload.SqlClient(
                f"http://127.0.0.1:{ports[0]}", database, timeout
            )
            wait_for_server(client, process, process_log_path, start_timeout)
            workload.setup_table(
                client,
                seed_rows,
                seed_batch_size,
                shards=shards,
                query_table=query_table_name,
                write_table=write_table_name,
                query_partitions=query_partitions,
                write_partitions=write_partitions,
                seed_timestamp_millis=seed_timestamp_millis,
            )
            query_workers = int(phase_cfg["query_workers"])
            write_workers = int(phase_cfg["write_workers"])
            write_delay = float(phase_cfg["write_delay_seconds"])

            result = workload.run_phase(
                client=client,
                name=phase_name,
                duration=float(duration_seconds),
                warmup=float(warmup_seconds),
                query_workers=query_workers,
                write_workers=write_workers,
                write_batch_size=write_batch_size,
                write_delay=write_delay,
                sequence=workload.Sequence(write_sequence_start_millis),
                scheduler_metrics="required" if enabled else "disabled",
                drain_timeout=float(drain_timeout_seconds),
                scrape_interval=scrape_interval,  # Always scrape for both baseline and scheduled
                metrics_dir=sample_root,
                query_sql=query_sql_param,
                write_table=write_table_name,
                shards=shards,
            )
        finally:
            stop_server(process)

    result["mode"] = mode
    result["sample"] = sample_number
    result["name"] = phase_name
    result["target_name"] = target_name
    result["scheduler_enabled"] = enabled

    # UNCONDITIONALLY overwrite sample.json with complete metadata attached by
    # benchmark (mode, iteration, phase, target, artifact_dir). run_phase may
    # have written an earlier version; this ensures final authoritative copy.
    sample_json_path = sample_root / "sample.json"
    sample_json_data = {
        "name": phase_name,
        "mode": mode,
        "sample": sample_number,
        "iteration": iteration,
        "phase": phase_name,
        "target": mode,
        "target_name": target_name,
        "scheduler_enabled": enabled,
        "duration_s": result.get("duration_s"),
        "warmup_s": result.get("warmup_s"),
        "drain_timeout_s": result.get("drain_timeout_s"),
        "workers": result.get("workers"),
        "requests": result.get("requests"),
        "polls": result.get("polls"),
        "poll_share": result.get("poll_share"),
        "scrape_records": result.get("scrape_records"),
        "scheduler_snapshots": result.get("scheduler_snapshots"),
        "artifact_dir": str(sample_root),
    }
    sample_json_path.write_text(json.dumps(sample_json_data, indent=2, sort_keys=True) + "\n")
    result["artifact_dir"] = str(sample_root)
    return result


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------


def build_performance_report(
    samples: list[dict[str, Any]],
    phases: list[str],
    max_regression_pct: float,
) -> dict[str, Any]:
    """Build the performance section of the report from completed samples.

    Capacities are derived from baseline query_only/write_only in each iteration.
    Evaluates worst same-iteration capacity-normalized change for every phase.
    Missing/zero/non-numeric capacities or missing/malformed pair produces an
    explicit invalid PerformanceEvaluation (never skipped/continued). No zero-
    defaults or pct_change=None-passed logic.

    Formulas:
      - query_only: scalar = query_rps / query_capacity; pct = (scheduled/baseline - 1)*100
      - write_only: scalar = write_rps / write_capacity; pct = (scheduled/baseline - 1)*100
      - light_write/saturated: scalar = query_rps/query_capacity + write_rps/write_capacity
        pct = (scheduled/baseline - 1)*100

    Duplicate detection: detects duplicate (iteration, phase, mode) pairs before
    dict insertion — no silent overwrite.
    """
    if not samples:
        return {"phases": [], "max_regression_pct": max_regression_pct, "status": "invalid"}

    # Group by iteration with duplicate detection
    iterations: dict[int, dict[str, dict[str, dict[str, float | None]]]] = {}
    dup_pairs: list[str] = []
    for sample in samples:
        iteration = sample.get("sample", 1)
        phase = sample.get("name", "")
        mode = sample.get("mode", "")
        if not isinstance(iteration, int):
            continue
        if iteration not in iterations:
            iterations[iteration] = {}
        if phase not in iterations[iteration]:
            iterations[iteration][phase] = {}
        if mode in iterations[iteration][phase]:
            dup_pairs.append(f"iteration={iteration} phase={phase} mode={mode}")
        reqs = sample.get("requests", {})
        iterations[iteration][phase][mode] = {
            "query_rps": (
                reqs.get("query", {}).get("successful_rps") if isinstance(reqs, dict) else None
            ),
            "write_rps": (
                reqs.get("write", {}).get("successful_rps") if isinstance(reqs, dict) else None
            ),
        }

    if dup_pairs:
        return {
            "phases": [],
            "max_regression_pct": max_regression_pct,
            "duplicates": dup_pairs,
            "status": "invalid",
        }

    performance_data: list[dict[str, Any]] = []
    for iteration in sorted(iterations):
        phase_data = iterations[iteration]
        # Capacities from baseline query_only query_rps and write_only write_rps
        qo = phase_data.get("query_only", {}).get("baseline", {})
        wo = phase_data.get("write_only", {}).get("baseline", {})
        raw_qc = qo.get("query_rps")
        raw_wc = wo.get("write_rps")

        query_capacity_valid = (
            raw_qc is not None
            and isinstance(raw_qc, (int, float))
            and math.isfinite(raw_qc)
            and raw_qc > 0
        )
        write_capacity_valid = (
            raw_wc is not None
            and isinstance(raw_wc, (int, float))
            and math.isfinite(raw_wc)
            and raw_wc > 0
        )

        for phase in phases:
            if phase == "query_only":
                # Scalar: query_rps / query_capacity
                bl = phase_data.get(phase, {}).get("baseline", {})
                sch = phase_data.get(phase, {}).get("scheduled", {})
                bl_qrps = bl.get("query_rps")
                sch_qrps = sch.get("query_rps")

                # Both baseline and scheduled must have finite positive values
                missing = []
                for name, val in [("baseline query_rps", bl_qrps), ("scheduled query_rps", sch_qrps)]:
                    if val is None:
                        missing.append(name)
                    elif not (isinstance(val, (int, float)) and math.isfinite(val)):
                        missing.append(f"{name} (non-finite: {val})")
                if not query_capacity_valid:
                    missing.append(f"query_capacity (query_rps={raw_qc})")

                if missing:
                    performance_data.append({
                        "phase": phase,
                        "iteration": iteration,
                        "baseline": {"query_rps": bl_qrps, "write_rps": None, "normalized": 0.0},
                        "scheduled": {"query_rps": sch_qrps, "write_rps": None, "normalized": 0.0},
                        "pct_change": None,
                        "passed": False,
                        "details": f"invalid: {', '.join(missing)}",
                    })
                    continue

                bl_norm = bl_qrps / raw_qc
                sch_norm = sch_qrps / raw_qc
                pct = (sch_norm / bl_norm - 1.0) * 100.0

                passed = True
                details = None
                if pct < -max_regression_pct:
                    passed = False
                    details = (
                        f"scheduled vs baseline change {pct:.2f}% "
                        f"is below -{max_regression_pct:.1f}%"
                    )

                performance_data.append({
                    "phase": phase,
                    "iteration": iteration,
                    "baseline": {"query_rps": bl_qrps, "write_rps": None, "normalized": bl_norm},
                    "scheduled": {"query_rps": sch_qrps, "write_rps": None, "normalized": sch_norm},
                    "pct_change": pct,
                    "passed": passed,
                    "details": details,
                })
                continue

            elif phase == "write_only":
                # Scalar: write_rps / write_capacity
                bl = phase_data.get(phase, {}).get("baseline", {})
                sch = phase_data.get(phase, {}).get("scheduled", {})
                bl_wrps = bl.get("write_rps")
                sch_wrps = sch.get("write_rps")

                missing = []
                for name, val in [("baseline write_rps", bl_wrps), ("scheduled write_rps", sch_wrps)]:
                    if val is None:
                        missing.append(name)
                    elif not (isinstance(val, (int, float)) and math.isfinite(val)):
                        missing.append(f"{name} (non-finite: {val})")
                if not write_capacity_valid:
                    missing.append(f"write_capacity (write_rps={raw_wc})")

                if missing:
                    performance_data.append({
                        "phase": phase,
                        "iteration": iteration,
                        "baseline": {"query_rps": None, "write_rps": bl_wrps, "normalized": 0.0},
                        "scheduled": {"query_rps": None, "write_rps": sch_wrps, "normalized": 0.0},
                        "pct_change": None,
                        "passed": False,
                        "details": f"invalid: {', '.join(missing)}",
                    })
                    continue

                bl_norm = bl_wrps / raw_wc
                sch_norm = sch_wrps / raw_wc
                pct = (sch_norm / bl_norm - 1.0) * 100.0

                passed = True
                details = None
                if pct < -max_regression_pct:
                    passed = False
                    details = (
                        f"scheduled vs baseline change {pct:.2f}% "
                        f"is below -{max_regression_pct:.1f}%"
                    )

                performance_data.append({
                    "phase": phase,
                    "iteration": iteration,
                    "baseline": {"query_rps": None, "write_rps": bl_wrps, "normalized": bl_norm},
                    "scheduled": {"query_rps": None, "write_rps": sch_wrps, "normalized": sch_norm},
                    "pct_change": pct,
                    "passed": passed,
                    "details": details,
                })
                continue

            # light_write or saturated: query_rps/query_capacity + write_rps/write_capacity
            pd = phase_data.get(phase, {})
            bl = pd.get("baseline", {})
            sch = pd.get("scheduled", {})

            bl_qrps = bl.get("query_rps")
            bl_wrps = bl.get("write_rps")
            sch_qrps = sch.get("query_rps")
            sch_wrps = sch.get("write_rps")

            # All four must be finite numeric
            missing = []
            for name, val in [("baseline query_rps", bl_qrps), ("baseline write_rps", bl_wrps),
                              ("scheduled query_rps", sch_qrps), ("scheduled write_rps", sch_wrps)]:
                if val is None:
                    missing.append(name)
                elif not (isinstance(val, (int, float)) and math.isfinite(val)):
                    missing.append(f"{name} (non-finite: {val})")

            if not query_capacity_valid:
                missing.append(
                    f"baseline query_capacity (query_rps={raw_qc})"
                )
            if not write_capacity_valid:
                missing.append(
                    f"baseline write_capacity (write_rps={raw_wc})"
                )

            if missing:
                performance_data.append({
                    "phase": phase,
                    "iteration": iteration,
                    "baseline": {"query_rps": bl_qrps, "write_rps": bl_wrps, "normalized": 0.0},
                    "scheduled": {"query_rps": sch_qrps, "write_rps": sch_wrps, "normalized": 0.0},
                    "pct_change": None,
                    "passed": False,
                    "details": f"invalid: {', '.join(missing)}",
                })
                continue

            bl_norm = bl_qrps / raw_qc + bl_wrps / raw_wc
            sch_norm = sch_qrps / raw_qc + sch_wrps / raw_wc

            # Guard: bl_norm must be finite and > 0, sch_norm finite and >= 0.
            # Zero/nonfinite baseline => invalid entry, never exception.
            if not (math.isfinite(bl_norm) and bl_norm > 0
                    and math.isfinite(sch_norm) and sch_norm >= 0):
                performance_data.append({
                    "phase": phase,
                    "iteration": iteration,
                    "baseline": {"query_rps": bl_qrps, "write_rps": bl_wrps, "normalized": bl_norm},
                    "scheduled": {"query_rps": sch_qrps, "write_rps": sch_wrps, "normalized": sch_norm},
                    "pct_change": None,
                    "passed": False,
                    "details": f"invalid: baseline normalized ({bl_norm}) must be finite positive, "
                               f"scheduled normalized ({sch_norm}) finite nonnegative",
                })
                continue

            pct = (sch_norm / bl_norm - 1.0) * 100.0

            passed = True
            details = None
            if pct < -max_regression_pct:
                passed = False
                details = (
                    f"scheduled vs baseline change {pct:.2f}% "
                    f"is below -{max_regression_pct:.1f}%"
                )

            performance_data.append(
                {
                    "phase": phase,
                    "iteration": iteration,
                    "baseline": {
                        "query_rps": bl_qrps,
                        "write_rps": bl_wrps,
                        "normalized": bl_norm,
                    },
                    "scheduled": {
                        "query_rps": sch_qrps,
                        "write_rps": sch_wrps,
                        "normalized": sch_norm,
                    },
                    "pct_change": pct,
                    "passed": passed,
                    "details": details,
                }
            )

    return {
        "phases": performance_data,
        "max_regression_pct": max_regression_pct,
    }


def build_artifacts_index(
    work_dir: Path,
    samples: list[dict[str, Any]],
    active_run_root: Path | None = None,
) -> dict[str, Any]:
    """Build a relative-path index of collected artifacts.

    For completed samples (no error), requires requests.jsonl,
    metrics/scrapes.jsonl, scrape-NNN.prom files, and sample.json.
    Missing required files for completed samples will list them
    with a missing_required field that affects status.

    *active_run_root* is the active run subtree (e.g. a reuse-<id> directory).
    When provided, only this subtree is included in ``run_dirs``, not historical
    sibling trees.
    """
    artifacts: dict[str, Any] = {
        "work_dir": str(work_dir),
        "runs_dir": str(work_dir / "runs"),
    }
    # Only list the active run root, not historical sibling trees
    runs_dir = work_dir / "runs"
    if active_run_root and active_run_root.exists():
        artifacts["run_dirs"] = [str(active_run_root.relative_to(work_dir))]
    elif runs_dir.exists():
        run_dirs = sorted(
            d for d in runs_dir.iterdir() if d.is_dir()
        )
        artifacts["run_dirs"] = [str(d.relative_to(work_dir)) for d in run_dirs]
    else:
        artifacts["run_dirs"] = []

    # Collect per-sample summary paths
    sample_refs: list[dict[str, Any]] = []
    for sample in samples:
        iteration = sample.get("sample", 0)
        phase = sample.get("name", "")
        mode = sample.get("mode", "")
        # Use actual artifact_dir if available, otherwise reconstruct
        artifact_dir_str = sample.get("artifact_dir")
        if artifact_dir_str:
            sample_dir = Path(artifact_dir_str)
        else:
            sample_dir = (
                work_dir / "runs" / f"iteration-{iteration:02d}" / phase / mode
            )
        ref: dict[str, Any] = {
            "iteration": iteration,
            "phase": phase,
            "mode": mode,
            "dir": str(sample_dir.relative_to(work_dir)) if sample_dir.is_relative_to(work_dir) else str(sample_dir),
        }
        # Check for required files in completed samples
        is_completed = "error" not in sample
        has_error = bool(sample.get("error"))

        # Attach config, process log, sample.json if they exist
        config_path = sample_dir / "config.toml"
        if config_path.exists():
            ref["config"] = str(config_path.relative_to(work_dir))
        proc_log = sample_dir / "process.log"
        if proc_log.exists():
            ref["process_log"] = str(proc_log.relative_to(work_dir))

        # Required files for completed samples
        metrics_dir = sample_dir / "metrics"
        if metrics_dir.exists():
            prom_files = sorted(metrics_dir.glob("*.prom"))
            ref["scrapes"] = [
                str(p.relative_to(work_dir)) for p in prom_files
            ]
            scrapes_jsonl = sample_dir / "scrapes.jsonl"
            if scrapes_jsonl.exists():
                ref["scrapes_jsonl"] = str(scrapes_jsonl.relative_to(work_dir))
        requests_jsonl = sample_dir / "requests.jsonl"
        if requests_jsonl.exists():
            ref["requests_jsonl"] = str(requests_jsonl.relative_to(work_dir))
        sample_json = sample_dir / "sample.json"
        if sample_json.exists():
            ref["sample_json"] = str(sample_json.relative_to(work_dir))

        if is_completed and not has_error:
            # Verify required files exist
            missing = []
            if "requests_jsonl" not in ref:
                missing.append("requests.jsonl")
            if "scrapes_jsonl" not in ref:
                missing.append("scrapes.jsonl")
            if "sample_json" not in ref:
                missing.append("sample.json")
            if "scrapes" not in ref or not ref["scrapes"]:
                missing.append("metrics/scrape-NNN.prom")
            if missing:
                ref["missing_required"] = missing

        sample_refs.append(ref)

    artifacts["samples"] = sample_refs
    return artifacts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Workload scheduler A/B benchmark."
    )
    p.add_argument(
        "--case",
        type=Path,
        default=BUILTIN_CASE,
        help="Path to case TOML (default: built-in workload_scheduler_2_8)",
    )
    p.add_argument(
        "--fixture-generator",
        type=Path,
        required=True,
        help="Path to query_perf_fixture binary",
    )
    p.add_argument(
        "--binary",
        type=Path,
        required=True,
        help="Path to greptime standalone binary",
    )
    p.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="Working directory for run artifacts and report",
    )
    p.add_argument(
        "--output",
        type=Path,
        help="Path for the JSON report (default: <work-dir>/report.json)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan only: invoke planner, validate scenario kind, print matrix, exit 0",
    )
    p.add_argument(
        "--no-gate",
        action="store_true",
        help="Diagnostic mode: run without gates, report only",
    )
    p.add_argument(
        "--reuse-work-dir",
        action="store_true",
        help="Allow nonempty work-dir; creates fresh run subdirectory",
    )
    p.add_argument(
        "--http-timeout",
        type=float,
        default=60.0,
        help="Per-request HTTP timeout in seconds",
    )
    p.add_argument(
        "--start-timeout",
        type=float,
        default=60.0,
        help="Server startup timeout in seconds",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

    # Resolve all paths
    case_path = args.case.resolve()
    fixture_generator = args.fixture_generator.resolve()
    binary = args.binary.resolve()
    work_dir = args.work_dir.resolve()
    output_path = args.output or work_dir / "report.json"

    if not case_path.is_file():
        # Route through structured report instead of direct error
        report = ws_report.build_report(
            errors=[f"case file not found: {case_path}"],
            config={
                "case": str(case_path),
                "fixture_generator": str(fixture_generator),
                "binary": str(binary),
                "work_dir": str(work_dir),
                "dry_run": args.dry_run,
                "no_gate": args.no_gate,
                "reuse_work_dir": args.reuse_work_dir,
            },
        )
        report["elapsed_seconds"] = 0.0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        sys.exit(report.get("exit_code", 2))

    if not fixture_generator.is_file():
        report = ws_report.build_report(
            errors=[f"fixture-generator not found: {fixture_generator}"],
            config={
                "case": str(case_path),
                "fixture_generator": str(fixture_generator),
                "binary": str(binary),
                "work_dir": str(work_dir),
                "dry_run": args.dry_run,
                "no_gate": args.no_gate,
                "reuse_work_dir": args.reuse_work_dir,
            },
        )
        report["elapsed_seconds"] = 0.0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        sys.exit(report.get("exit_code", 2))

    # Load plan from Rust planner
    try:
        plan = load_plan(fixture_generator, case_path)
    except (RuntimeError, json.JSONDecodeError) as e:
        report = ws_report.build_report(
            errors=[f"plan loading failed: {e}"],
        )
        report["elapsed_seconds"] = 0.0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        sys.exit(report.get("exit_code", 2))

    try:
        validate_scenario_kind(plan)
    except ValueError as e:
        report = ws_report.build_report(
            errors=[f"scenario validation failed: {e}"],
        )
        report["elapsed_seconds"] = 0.0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        sys.exit(report.get("exit_code", 2))

    # Strictly validate every required field in the normalized scenario JSON.
    # Missing/wrong-type fields are fatal invalid-plan errors.
    try:
        validate_normalized_scenario(plan)
    except ValueError as e:
        report = ws_report.build_report(
            errors=[f"invalid normalized plan: {e}"],
        )
        report["elapsed_seconds"] = 0.0
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        sys.exit(report.get("exit_code", 2))

    config = extract_config(plan)
    phases_list = extract_phases(plan)
    targets_list = extract_targets(plan)
    phase_names = [p.get("name", "") for p in phases_list]
    iterations = int(config["iterations"])

    # Validate phases match expectations in gated mode
    if not args.no_gate and not args.dry_run:
        missing = [p for p in REQUIRED_PHASES if p not in phase_names]
        if missing or len(phase_names) != 4:
            err_msg = ""
            if missing:
                err_msg = f"missing required phases: {', '.join(missing)}"
            if len(phase_names) != 4:
                phase_err = f"expected exactly 4 phases, got {len(phase_names)}"
                err_msg = f"{err_msg}; {phase_err}" if err_msg else phase_err
            report = ws_report.build_report(
                errors=[err_msg],
            )
            report["elapsed_seconds"] = 0.0
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(report, indent=2, sort_keys=True))
            sys.exit(report.get("exit_code", 2))

    if args.dry_run:
        # Print deterministic execution plan using shared matrix helper
        matrix = build_matrix(iterations, phases_list, targets=targets_list)
        print(json.dumps(
            {
                "status": "planned",
                "case": str(case_path),
                "scenario_kind": "workload_scheduler",
                "iterations": iterations,
                "phases": [p.get("name", "") for p in phases_list],
                "matrix": matrix,
                "config": config,
            },
            indent=2,
        ))
        sys.exit(0)

    # Manage work directory (Issue 7: fresh mode rejects ANY nonempty work-dir)
    runs_dir = work_dir / "runs"
    use_fresh_subtree = False
    fresh_subtree_path: Path | None = None

    # Check if work-dir has ANY nonempty content (not just runs/)
    def _work_dir_is_nonempty() -> bool:
        if not work_dir.exists():
            return False
        try:
            for _ in work_dir.iterdir():
                return True
        except (OSError, FileNotFoundError):
            pass
        return False

    if _work_dir_is_nonempty():
        if not args.reuse_work_dir:
            report = ws_report.build_report(
                errors=[f"work-dir {work_dir} is not empty; use --reuse-work-dir to allow"],
                config={
                    "case": str(case_path),
                    "fixture_generator": str(fixture_generator),
                    "binary": str(binary),
                    "work_dir": str(work_dir),
                    "dry_run": args.dry_run,
                    "no_gate": args.no_gate,
                    "reuse_work_dir": args.reuse_work_dir,
                },
            )
            report["elapsed_seconds"] = 0.0
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(report, indent=2, sort_keys=True))
            sys.exit(report.get("exit_code", 2))

        # Create a fresh unique run subtree for reuse mode
        reuse_run_id = int(time.time() * 1000000)
        fresh_subtree_path = work_dir / "runs" / f"reuse-{reuse_run_id}"
        runs_dir = fresh_subtree_path
        use_fresh_subtree = True
    runs_dir.mkdir(parents=True, exist_ok=True)

    # Check binary presence (not required in dry-run)
    if not args.dry_run:
        if not binary.is_file():
            report = ws_report.build_report(
                errors=[f"binary does not exist: {binary}"],
            )
            report["elapsed_seconds"] = 0.0
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(report, indent=2, sort_keys=True))
            sys.exit(report.get("exit_code", 2))
        if not os.access(str(binary), os.X_OK):
            report = ws_report.build_report(
                errors=[f"binary is not executable: {binary}"],
            )
            report["elapsed_seconds"] = 0.0
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            print(json.dumps(report, indent=2, sort_keys=True))
            sys.exit(report.get("exit_code", 2))

    # Run all samples — iterate build_matrix() output (single source of truth)
    samples: list[dict[str, Any]] = []
    start_time = time.monotonic()
    try:
        matrix = build_matrix(iterations, phases_list, targets=targets_list)
        phases_lookup = {p.get("name", ""): p for p in phases_list}
        for entry in matrix:
            iteration = entry["iteration"]
            phase_name = entry["phase"]
            phase_cfg = phases_lookup.get(phase_name, {})
            for target_info in entry["entries"]:
                target_name = target_info["name"]
                enabled = target_info["scheduler_enabled"]
                print(
                    f"iteration={iteration} phase={phase_name} "
                    f"mode={target_name}",
                    file=sys.stderr,
                    flush=True,
                )
                try:
                    result = run_sample(
                        binary=binary,
                        root=runs_dir,
                        phase_cfg=phase_cfg,
                        enabled=enabled,
                        target_name=target_name,
                        iteration=iteration,
                        sample_number=iteration,
                        config_overlay=config,
                        timeout=args.http_timeout,
                        start_timeout=args.start_timeout,
                    )
                except (RuntimeError, TimeoutError, OSError) as e:
                    # Record failure as an error sample and continue
                    result = {
                        "name": phase_name,
                        "mode": target_name,
                        "sample": iteration,
                        "error": str(e),
                    }
                samples.append(result)

        elapsed = time.monotonic() - start_time
    except BaseException as e:
        # Any uncaught runtime/subprocess/filesystem failure - exit 3 for operational
        report = ws_report.build_report(
            config={
                "case": str(case_path),
                "fixture_generator": str(fixture_generator),
                "binary": str(binary),
                "work_dir": str(work_dir),
                "dry_run": args.dry_run,
                "no_gate": args.no_gate,
                "reuse_work_dir": args.reuse_work_dir,
                "reuse_subtree": str(fresh_subtree_path) if use_fresh_subtree else None,
            },
            artifacts=build_artifacts_index(work_dir, samples, active_run_root=runs_dir if use_fresh_subtree else None),
            samples=samples,
            errors=[f"unexpected failure: {e}"],
        )
        report["elapsed_seconds"] = time.monotonic() - start_time
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, indent=2, sort_keys=True))
        sys.exit(3)

    # Build report — persist reload + per-iteration mechanism + artifact validation
    errors = [s.get("error", "") for s in samples if "error" in s]
    errors = [e for e in errors if e]

    # Feed build_artifacts_index().missing_required into combined invalid status (Issue 3)
    artifacts_invalid_failures: list[str] = []

    # Reload persisted artifacts for authoritative cross-check (Issue 3)
    artifacts_index = build_artifacts_index(work_dir, samples, active_run_root=runs_dir if use_fresh_subtree else None)
    for sample in samples:
        if "error" in sample:
            continue
        artifact_dir_str = sample.get("artifact_dir")
        if not artifact_dir_str:
            continue
        sample_dir = Path(artifact_dir_str)

        # Build expected_metadata from planned values for validation.
        expected_meta: dict[str, object] = {
            "name": str(sample.get("name", "")),
            "mode": str(sample.get("mode", "")),
            "sample": sample.get("sample", 0),
            "iteration": sample.get("sample", 0),
            "phase": str(sample.get("name", "")),
            "target": str(sample.get("mode", "")),
            "target_name": str(sample.get("target_name", "")),
            "scheduler_enabled": bool(sample.get("scheduler_enabled", False)),
            "artifact_dir": str(sample_dir),
            # Exact normalized timing plan
            "duration_s": config.get("duration_seconds"),
            "warmup_s": config.get("warmup_seconds"),
            "drain_timeout_s": config.get("drain_timeout_seconds"),
        }
        workers_val = sample.get("workers")
        if isinstance(workers_val, dict) and workers_val:
            expected_meta["workers"] = dict(workers_val)

        # Use canonical loader — single source of truth
        persisted = ws_report.load_persisted_sample(
            sample_dir,
            expected_metadata=expected_meta,
        )
        if persisted.validation.status != "passed":
            artifacts_invalid_failures.append(
                f"{sample.get('name', '?')}/{sample.get('mode', '?')}/iteration-{sample.get('sample', '?')}: "
                f"load_persisted_sample: {', '.join(persisted.validation.failures)}"
            )
    for sref in artifacts_index.get("samples", []):
        missing = sref.get("missing_required", [])
        if missing:
            prefix = f"iteration-{sref.get('iteration')}/{sref.get('phase')}/{sref.get('mode')}"
            artifacts_invalid_failures.extend(
                f"{prefix}: missing required artifact: {m}" for m in missing
            )

    # Invoke validate_sample_artifacts for every sample (Issue 3)
    for sample in samples:
        if "error" in sample:
            continue
        artifact_dir_str = sample.get("artifact_dir")
        if artifact_dir_str:
            mode = sample.get("mode", "unknown")
            scheduler_metrics_required = mode == "scheduled"
            av = ws_report.validate_sample_artifacts(
                sample,
                scheduler_metrics_required=scheduler_metrics_required,
            )
            if av.status != "passed":
                artifacts_invalid_failures.extend(av.failures)

    # Build canonical samples solely from loader-returned payload (no dict(sample) copy).
    # All timing fields are guaranteed present by validate_normalized_scenario.
    scrape_interval = config["scrape_interval_seconds"]
    duration_seconds = config["duration_seconds"]
    expected_scrape_count = int(config["expected_scrape_count"])
    iteration_scrape_failures: list[str] = []
    canonical_samples: list[dict[str, Any]] = []
    reloaded_synthetic_samples: dict[tuple[int, str, str], dict[str, Any]] = {}
    for sample in samples:
        if "error" in sample:
            continue
        artifact_dir_str = sample.get("artifact_dir")
        if not artifact_dir_str:
            continue
        sample_dir = Path(artifact_dir_str)
        iteration = sample.get("sample", 0)
        phase_name = sample.get("name", "")
        mode = sample.get("mode", "")
        key = (iteration, phase_name, mode)

        # Build expected_metadata from planned values for validation.
        expected_meta: dict[str, object] = {
            "name": str(phase_name),
            "mode": str(mode),
            "sample": iteration,
            "iteration": iteration,
            "phase": str(phase_name),
            "target": str(mode),
            "target_name": str(sample.get("target_name", "")),
            "scheduler_enabled": bool(sample.get("scheduler_enabled", False)),
            "artifact_dir": str(sample_dir),
            # Exact normalized timing plan
            "duration_s": config.get("duration_seconds"),
            "warmup_s": config.get("warmup_seconds"),
            "drain_timeout_s": config.get("drain_timeout_seconds"),
        }
        workers_val = sample.get("workers")
        if isinstance(workers_val, dict) and workers_val:
            expected_meta["workers"] = dict(workers_val)

        # Use canonical loader — single source of truth.
        # Original in-memory sample is only used to locate sample_dir and provide
        # expected_metadata before reload; after loading, ALL canonical keys
        # derive from disk.
        persisted = ws_report.load_persisted_sample(
            sample_dir,
            expected_metadata=expected_meta,
        )

        # If loader reports invalid, propagate failures but still build partial entry.
        if persisted.validation.status != "passed":
            artifacts_invalid_failures.append(
                f"{phase_name}/{mode}/iteration-{iteration}: "
                f"load_persisted_sample: {', '.join(persisted.validation.failures)}"
            )

        # Construct canonical sample payload SOLELY from loader-returned data.
        # ALL keys derive from persisted.metadata, not from in-memory copies.
        canonical_name = str(persisted.metadata.get("name", phase_name))
        canonical_mode = str(persisted.metadata.get("mode", mode))
        canonical_sample = persisted.metadata.get("sample", iteration)
        if isinstance(canonical_sample, (int, float)):
            canonical_sample = int(canonical_sample)

        canonical: dict[str, Any] = {
            "name": canonical_name,
            "mode": canonical_mode,
            "sample": canonical_sample,
            "artifact_dir": str(sample_dir),
            "workers": persisted.metadata.get("workers", sample.get("workers", {})),
            "requests": persisted.request_summaries,
            "scrape_records": persisted.scrape_records,
            "scheduler_snapshots": persisted.scheduler_snapshots,
        }
        # Merge additional canonical metadata fields from persisted.metadata
        for meta_key in ("target_name", "scheduler_enabled", "duration_s",
                         "warmup_s", "drain_timeout_s", "polls", "poll_share",
                         "iteration", "phase", "target"):
            val = persisted.metadata.get(meta_key)
            if val is not None:
                canonical[meta_key] = val
            elif meta_key in ("iteration",):
                canonical[meta_key] = canonical_sample
            elif meta_key in ("phase",):
                canonical[meta_key] = canonical_name

        reloaded_synthetic_samples[key] = canonical
        canonical_samples.append(canonical)

        # Validate iteration scrape using reloaded data
        if not args.dry_run:
            scheduler_metrics_required = mode == "scheduled"
            val = ws_report.validate_iteration_scrape(
                canonical,
                expected_scrape_count=expected_scrape_count,
                scrape_interval=scrape_interval,
                duration=duration_seconds,
                scheduler_metrics_required=scheduler_metrics_required,
            )
            if val.status != "passed":
                iteration_scrape_failures.extend(
                    f"{val.phase}/{val.target_mode}/iteration-{val.iteration}: {f}"
                    for f in val.failures
            )

    # Request validity check — using ONLY canonical (persisted) samples.
    # All gates fields are guaranteed present by validate_normalized_scenario.
    request_eval = ws_report.evaluate_request_validity(
        canonical_samples,
        max_failure_rate=config["gates"]["max_failure_rate"],
        max_outstanding_requests=config["gates"]["max_outstanding_requests"],
    )

    # Under --no-gate, suppress well-formed request threshold failures only.
    # Invalid/error request_eval status (integrity/schema errors) is preserved.
    if args.no_gate and request_eval is not None and request_eval.status == "failed":
        request_eval = ws_report.RequestEvaluation(status="passed", failures=())

    # ------------------------------------------------------------------
    # Mechanism evaluation: per-iteration, using ONLY reloaded persisted data
    # ------------------------------------------------------------------
    def _iteration_mechanism_eval(
        iteration: int,
        synthetic_samples: dict[tuple[int, str, str], dict[str, Any]],
        config: dict[str, Any],
    ) -> ws_report.SchedulerEvaluation | None:
        """Evaluate scheduler mechanism for ONE iteration using ONLY reloaded persisted data."""
        phase_summaries: dict[str, ws_report.SchedulerSummary | None] = {}
        for req_phase in REQUIRED_PHASES:
            key = (iteration, req_phase, "scheduled")
            synthetic = synthetic_samples.get(key)
            if synthetic is None:
                phase_summaries[req_phase] = None
                continue
            snapshots_raw = synthetic.get("scheduler_snapshots", [])
            snapshots = [
                ws_report.SchedulerMetrics(
                    polls=dict(s.get("polls", {})),
                    queued=dict(s.get("queued", {})),
                    active=s.get("active"),
                )
                for s in snapshots_raw
                if isinstance(s, dict)
            ]
            if not snapshots:
                phase_summaries[req_phase] = None
            else:
                phase_summaries[req_phase] = ws_report.summarize_scheduler_metrics(snapshots)

        # If all four phases are present, evaluate
        present = all(
            phase_summaries.get(p) is not None
            for p in REQUIRED_PHASES
        )
        if not present:
            missing = [p for p in REQUIRED_PHASES if phase_summaries.get(p) is None]
            return ws_report.SchedulerEvaluation(
                status="invalid",
                passed=False,
                exit_code=2,
                failures=(f"iteration {iteration}: missing required phases: {', '.join(missing)}",),
            )

        # All gates and scheduler fields are guaranteed present by
        # validate_normalized_scenario — no fallback defaults.
        gates = config["gates"]
        scheduler_cfg = config["scheduler"]
        thresholds = ws_report.SchedulerGateThresholds(
            required_phases=REQUIRED_PHASES,
            write_share_min=gates["dual_backlog_lower"],
            write_share_max=gates["dual_backlog_upper"],
            min_dual_backlog_interval_fraction=gates["min_dual_backlog_interval_fraction"],
            min_dual_backlog_polls_per_class=int(gates["min_dual_backlog_polls_per_class"]),
            single_class_purity_min_share=gates["min_single_class_active_purity"],
            max_active_polls=int(scheduler_cfg["max_concurrent_polls"]),
            max_failure_rate=gates["max_failure_rate"],
            max_outstanding_requests=int(gates["max_outstanding_requests"]),
            min_light_write_query_share=gates["min_light_write_query_share"],
            active_within_scheduler_limit=gates["active_within_scheduler_limit"],
            max_capacity_normalized_regression_pct=gates["max_capacity_normalized_regression_pct"],
        )
        return ws_report.evaluate_scheduler_report(
            phase_summaries, thresholds, errors=()
        )

    mechanism_eval: ws_report.SchedulerEvaluation | None = None

    iteration_evals: list[ws_report.SchedulerEvaluation] = []
    for iteration in range(1, iterations + 1):
        ie = _iteration_mechanism_eval(iteration, reloaded_synthetic_samples, config)
        if ie is not None:
            iteration_evals.append(ie)

    if not iteration_evals:
        mechanism_eval = ws_report.SchedulerEvaluation(
            status="invalid",
            passed=False,
            exit_code=2,
            failures=("no iterations evaluated",),
        )
    else:
        # Combine all iteration statuses using rank precedence
        all_statuses = [ie.status for ie in iteration_evals]
        combined_status = ws_report.combine_statuses(*all_statuses)
        all_failures: list[str] = []
        for ie in iteration_evals:
            all_failures.extend(ie.failures)
        mechanism_eval = ws_report.SchedulerEvaluation(
            status=combined_status,
            passed=combined_status == "passed",
            exit_code=ws_report.exit_code_for_status(combined_status),
            failures=tuple(all_failures),
        )

    # Under --no-gate, suppress only well-formed mechanism/performance failed, never invalid/error
    if args.no_gate:
        if mechanism_eval is not None and mechanism_eval.status == "failed":
            mechanism_eval = ws_report.SchedulerEvaluation(
                status="passed",
                passed=True,
                exit_code=0,
                failures=(),
            )

    # Performance evaluation
    perf_data = build_performance_report(
        canonical_samples,
        REQUIRED_PHASES,
        float(config["gates"]["max_capacity_normalized_regression_pct"]),
    )

    # Build performance evaluations list (separate integrity invalid from regression failed)
    perf_eval_list: list[ws_report.PerformanceEvaluation] = []
    for p in perf_data.get("phases", []):
        is_integrity = p.get("details", "").startswith("invalid:") if p.get("details") else False
        passed = p["passed"]
        details = p["details"]
        if args.no_gate and not is_integrity and not passed:
            # Under --no-gate, regression failures are suppressed (passed=True)
            passed = True
            details = None
        perf_eval_list.append(
            ws_report.PerformanceEvaluation(
                phase=p["phase"],
                iteration=p["iteration"],
                baseline_normalized=p["baseline"]["normalized"],
                scheduled_normalized=p["scheduled"]["normalized"],
                pct_change=p["pct_change"],
                passed=passed,
                details=details,
            )
        )

    # Determine performance integrity vs gate failure status
    # Missing/duplicate/nonfinite/noncomputable => invalid; regression => failed
    perf_entries = perf_data.get("phases", [])
    perf_integrity_failures: list[str] = [
        e["details"] for e in perf_entries
        if not e["passed"] and e.get("details", "").startswith("invalid:")
    ]
    perf_regression_failures: list[str] = [
        e["details"] for e in perf_entries
        if not e["passed"] and e.get("details") and not e["details"].startswith("invalid:")
    ]

    # Artifact validation via report module
    artifact_validation = None
    if not args.no_gate:
        av_failures: list[str] = list(artifacts_invalid_failures)
        av_failures.extend(iteration_scrape_failures)
        artifact_validation = ws_report.ArtifactValidation(
            status="passed" if not av_failures else "invalid",
            failures=tuple(av_failures),
        )
    else:
        # Even with --no-gate, invalid evidence/config still propagates (Issue 7)
        av_failures = list(artifacts_invalid_failures)
        av_failures.extend(iteration_scrape_failures)
        if av_failures:
            artifact_validation = ws_report.ArtifactValidation(
                status="invalid",
                failures=tuple(av_failures),
            )


    report = ws_report.build_report(
        config={
            "case": str(case_path),
            "fixture_generator": str(fixture_generator),
            "binary": str(binary),
            "work_dir": str(work_dir),
            "iterations": iterations,
            "phases": phase_names,
            "output": str(output_path),
            "dry_run": args.dry_run,
            "no_gate": args.no_gate,
            "reuse_work_dir": args.reuse_work_dir,
            "reuse_subtree": str(fresh_subtree_path) if use_fresh_subtree else None,
            "elapsed_seconds": elapsed,
        },
        artifacts=build_artifacts_index(work_dir, canonical_samples, active_run_root=runs_dir if use_fresh_subtree else None),
        samples=canonical_samples,
        request_eval=request_eval,
        mechanism_eval=mechanism_eval,
        artifact_validation=artifact_validation,
        performance_evals=perf_eval_list,
        errors=errors if errors else None,
    )

    report["performance"] = perf_data

    # Write report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    # Always print to stdout
    print(json.dumps(report, indent=2, sort_keys=True))

    # Exit with status code based on report
    status = report.get("status", "error")
    exit_code = report.get("exit_code", 1)
    if args.dry_run:
        sys.exit(0)
    if args.no_gate:
        # --no-gate suppresses only observed gate failure (status "failed"),
        # but NEVER operational error (3) or invalid evidence/config (2).
        if status in ("error", "invalid"):
            sys.exit(exit_code)
        sys.exit(0)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
