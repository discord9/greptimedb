#!/usr/bin/env -S uv run --script
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

# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///
"""
run_removed_file_gc_scale.py
Test B harness: real removed-file delete/report pressure at scale.

Generates many real SSTs via repeated INSERT + ADMIN FLUSH_TABLE,
compacts, runs ADMIN GC_REGIONS (fast mode), captures metrics/logs,
and writes evidence to the output directory.

Run with: uv run ...run_removed_file_gc_scale.py --count 1000 --start 0 --out /tmp/gc-test-b-Nk/

See docs/how-to/how-to-test-gc-huge-file-regions.md for the runbook.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---- import S3Transport from sibling helper --------------------------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
from write_dummy_region_objects import S3Transport  # type: ignore[import-not-found]

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Test B: removed-file GC scale harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Required
    p.add_argument("--count", type=int, required=True,
                   help="Number of INSERT+FLUSH iterations (e.g. 1000)")
    p.add_argument("--start", type=int, required=True,
                   help="Starting value for INSERT (e.g. 0)")
    p.add_argument("--out", required=True,
                   help="Output directory for evidence files")

    # Cluster
    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--table", default="gc_hf_test_b")
    g.add_argument("--frontend-service", default="gc-stress-greptimedb-frontend")
    g.add_argument("--monitor-service", default="gc-stress-greptimedb-monitor-standalone")
    g.add_argument("--minio-service", default="minio")
    g.add_argument("--cluster-name", default="gc-stress-greptimedb")

    # Ports
    g = p.add_argument_group("ports")
    g.add_argument("--frontend-local-port", type=int, default=14000)
    g.add_argument("--frontend-remote-port", type=int, default=4000)
    g.add_argument("--monitor-local-port", type=int, default=15000)
    g.add_argument("--monitor-remote-port", type=int, default=4000)
    g.add_argument("--minio-local-port", type=int, default=19000)
    g.add_argument("--minio-remote-port", type=int, default=9000)

    # S3
    g = p.add_argument_group("s3 (for object counting)")
    g.add_argument("--s3-endpoint", default="http://127.0.0.1:19000")
    g.add_argument("--s3-access-key", default=os.environ.get("GC_STRESS_S3_ACCESS_KEY", "rootuser"))
    g.add_argument("--s3-secret-key", default=os.environ.get("GC_STRESS_S3_SECRET_KEY", ""),
                   help="S3 secret key, or set GC_STRESS_S3_SECRET_KEY")
    g.add_argument("--bucket", default="gc-stress-bucket")
    g.add_argument("--root-prefix", default="gc-hf-lab")

    # Rate
    g = p.add_argument_group("rate")
    g.add_argument("--progress-every", type=int, default=1000,
                   help="Print progress every N INSERTs (default: 1000)")
    g.add_argument("--logs-since", default="6h",
                   help="kubectl logs --since duration (default: 6h)")
    g.add_argument("--http-timeout", type=float, default=300.0,
                   help="HTTP timeout for SQL requests in seconds (default: 300)")
    g.add_argument("--resume-after-compact", action="store_true",
                   help="Skip INSERT+FLUSH and COMPACT; start from current S3 state before GC")

    return p


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def jstr(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


def write_json(path: str, obj: Any) -> None:
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def write_text(path: str, text: str) -> None:
    with open(path, "w") as f:
        f.write(text)


def timed(func, *args, **kwargs) -> Tuple[Any, float]:
    """Execute func and return (result, elapsed_seconds)."""
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    return result, elapsed


# ---- Port-forward ---------------------------------------------------------
def port_forward(local: int, remote: int, namespace: str, service: str,
                 out_dir: Path) -> Tuple[subprocess.Popen, Any]:
    cmd = [
        "kubectl", "port-forward", "-n", namespace,
        f"svc/{service}", f"{local}:{remote}",
    ]
    # Do not use PIPE unless a reader drains it. `kubectl port-forward` writes
    # connection lifecycle messages; an undrained pipe can fill and block the
    # port-forward process, making subsequent HTTP requests hang before they
    # reach GreptimeDB.
    log = open(out_dir / f"port-forward-{service}.log", "ab")
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    return proc, log


def wait_for_port(port: int, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.5)
    return False


# ---- HTTP: SQL & Prometheus -----------------------------------------------
def _http_post(url: str, body: bytes, content_type: str,
               timeout: float) -> Tuple[int, str]:
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode(errors="replace")


def sql_query(port: int, sql: str, timeout: float = 300.0) -> Tuple[int, str]:
    url = f"http://127.0.0.1:{port}/v1/sql"
    data = urllib.parse.urlencode({"sql": sql}).encode()
    return _http_post(url, data, "application/x-www-form-urlencoded", timeout)


def prom_query(port: int, query: str) -> Tuple[int, str]:
    url = f"http://127.0.0.1:{port}/v1/prometheus/api/v1/query"
    params = urllib.parse.urlencode({"query": query})
    full_url = f"{url}?{params}"
    with urllib.request.urlopen(full_url, timeout=30) as resp:
        return resp.status, resp.read().decode(errors="replace")


def prom_snapshot(port: int, query: str, out_dir: str, tag: str) -> float:
    """Snapshot a Prometheus instant query.  Returns snapshot time (epoch s)."""
    t0 = time.time()
    code, text = prom_query(port, query)
    elapsed = time.time() - t0
    data = {"query": query, "status_code": code, "response": json.loads(text)}
    fname = f"{tag}.json"
    write_json(os.path.join(out_dir, fname), data)
    write_text(os.path.join(out_dir, f"{tag}-snapshot-time.txt"),
               f"{time.time()}\n")
    return time.time()


# ---- kubectl helpers ------------------------------------------------------
def kubectl(args: List[str]) -> str:
    return subprocess.check_output(["kubectl"] + args, text=True)


def kubectl_json(args: List[str]) -> Any:
    return json.loads(kubectl(args + ["-o", "json"]))


def kubectl_logs(pod: str, namespace: str, since: str,
                 container: Optional[str] = None) -> str:
    cmd = ["kubectl", "logs", "-n", namespace, pod, f"--since={since}"]
    if container:
        cmd.extend(["-c", container])
    return subprocess.check_output(cmd, text=True)


def kubectl_pod_name(label: str, namespace: str) -> str:
    """Get first pod name matching a label selector."""
    out = kubectl(["get", "pod", "-n", namespace, "-l", label,
                    "-o", "jsonpath={.items[0].metadata.name}"])
    name = out.strip()
    if not name:
        fail(f"No pod found for label '{label}' in namespace '{namespace}'")
    return name


# ---- S3 counting ----------------------------------------------------------
def count_s3_objects(transport: S3Transport, bucket: str, prefix: str
                     ) -> Dict[str, int]:
    """Count total, parquet, and manifest objects under prefix."""
    total = 0
    parquet = 0
    manifest = 0
    for key, _ in transport.list_objects_v2(bucket, prefix):
        total += 1
        if key.endswith(".parquet"):
            parquet += 1
        elif "/manifest/" in key:
            manifest += 1
    return {"total": total, "parquet": parquet, "manifest": manifest}


# ---- GC report parsing ----------------------------------------------------
def _find_field(line: str, key: str) -> Optional[str]:
    m = re.search(rf'"{key}":\s*"([^"]*)"', line)
    return m.group(1) if m else None


def parse_gc_reports(meta_logs: str, region_id: int) -> List[Dict[str, Any]]:
    """Extract GC report summaries for the target region from meta logs."""
    summaries: List[Dict[str, Any]] = []
    lines = meta_logs.splitlines()
    for i, line in enumerate(lines):
        if "GC report:" not in line:
            continue
        # Check this report mentions our region
        if f"{region_id}" not in line:
            continue
        ts = _find_field(line, "timestamp") or ""
        file_id_count = line.count("FileId(")
        byte_len = len(line)
        # Check previous line for retry count
        need_retry_empty = True
        processed_region = True
        if i > 0 and "retry region count:" in lines[i - 1]:
            m_retry = re.search(r"retry region count:\s*(\d+)", lines[i - 1])
            if m_retry and int(m_retry.group(1)) > 0:
                need_retry_empty = False
        summaries.append({
            "timestamp": ts,
            "byte_len": byte_len,
            "file_id_count": file_id_count,
            "need_retry_empty": need_retry_empty,
            "processed_region": processed_region,
        })
    return summaries


# ---- Metrics diff ---------------------------------------------------------
def _extract_series(response: Dict) -> List[Dict]:
    results = response.get("data", {}).get("result", [])
    out = []
    for r in results:
        metric = r.get("metric", {})
        value = r.get("value", [None, "0"])
        try:
            v = float(value[1])
        except (TypeError, ValueError):
            v = 0.0
        out.append({"metric": metric.get("__name__", "?"), "labels": metric, "value": v})
    return out


def _diff_series(before: List[Dict], after: List[Dict]) -> List[Dict]:
    before_map: Dict[str, Dict] = {}
    for s in before:
        key = json.dumps(s["labels"], sort_keys=True)
        before_map[key] = s
    changed = []
    for s in after:
        key = json.dumps(s["labels"], sort_keys=True)
        b = before_map.get(key)
        if b is None:
            changed.append({
                "metric": s["metric"], "labels": s["labels"],
                "before": None, "after": s["value"], "delta": s["value"],
            })
        elif abs(s["value"] - b["value"]) > 1e-9:
            changed.append({
                "metric": s["metric"], "labels": s["labels"],
                "before": b["value"], "after": s["value"],
                "delta": s["value"] - b["value"],
            })
    return changed


def metrics_snapshot_set(port: int, queries: Dict[str, str],
                         out_dir: str, tag: str) -> Dict[str, Any]:
    """Snapshot multiple Prometheus queries.  Returns {name: response_dict}."""
    results: Dict[str, Any] = {}
    for name, query in queries.items():
        code, text = prom_query(port, query)
        data = json.loads(text)
        fname = f"{tag}-{name}.json"
        write_json(os.path.join(out_dir, fname), {"query": query, "response": data})
        results[name] = data
    return results


def compute_metrics_delta(before: Dict[str, Any], after: Dict[str, Any]
                          ) -> Dict[str, Any]:
    """Compute per-query metrics delta."""
    delta: Dict[str, Any] = {}
    for name in before:
        b_resp = before.get(name, {})
        a_resp = after.get(name, {})
        b_series = _extract_series(b_resp)
        a_series = _extract_series(a_resp)
        changed = _diff_series(b_series, a_series)
        delta[name] = {
            "before_series": len(b_series),
            "after_series": len(a_series),
            "changed_or_new": changed,
        }
    return delta


# ---- Summary formatting ----------------------------------------------------
def build_concise_summary(
    out_dir: str,
    args: argparse.Namespace,
    table_id: int,
    region_id: int,
    prefix: str,
    object_counts: Dict[str, Dict[str, int]],
    elapsed: Dict[str, float],
    metrics_delta: Dict[str, Any],
    gc_reports: List[Dict[str, Any]],
    pods_after: Any,
    cluster_after: str,
) -> str:
    """Build concise-summary.txt matching Test B conventions."""
    lines = [
        f"EVIDENCE_DIR {out_dir}",
        f"TABLE {args.table}",
        f"TABLE_ID {table_id}",
        f"REGION_ID {region_id}",
        f"PREFIX {prefix}",
        f"COUNTS {json.dumps(object_counts, sort_keys=True)}",
    ]
    for label in ["insert_flush", "compact", "gc", "second_gc"]:
        e = elapsed.get(label)
        if e is not None:
            lines.append(f"{label.upper()}_ELAPSED_SECONDS {e}")

    # Extract key metrics deltas
    md = metrics_delta.get("mito_gc_all", {}).get("changed_or_new", [])
    for c in md:
        name = c["metric"]
        labels = c.get("labels", {})
        stage = labels.get("stage", "")
        mode = labels.get("mode", "")
        file_type = labels.get("file_type", "")
        if name == "greptime_mito_gc_files_deleted_total" and file_type:
            lines.append(
                f"mito files_deleted {file_type}: "
                f"{c.get('before')} -> {c.get('after')} delta={c.get('delta')}"
            )
        elif name == "greptime_mito_gc_delete_file_count":
            lines.append(
                f"mito delete_file_count: "
                f"{c.get('before')} -> {c.get('after')} delta={c.get('delta')}"
            )
        elif name == "greptime_mito_gc_runs_total" and mode:
            lines.append(
                f"mito gc runs {mode}: "
                f"{c.get('before')} -> {c.get('after')} delta={c.get('delta')}"
            )
        elif name == "greptime_mito_gc_duration_seconds_sum" and stage:
            lines.append(f"mito {stage} duration delta={c.get('delta')}")

    od_count = metrics_delta.get("opendal_s3_delete_count", {}).get("changed_or_new", [])
    for c in od_count:
        lines.append(
            f"OpenDAL S3 delete count: "
            f"{c.get('before')} -> {c.get('after')} delta={c.get('delta')}"
        )
    od_sum = metrics_delta.get("opendal_s3_delete_sum", {}).get("changed_or_new", [])
    for c in od_sum:
        lines.append(f"OpenDAL S3 delete duration sum delta={c.get('delta')}")

    # GC report summary (last 8)
    lines.append(f"GC_REPORT_LOG_SUMMARY {json.dumps(gc_reports[-8:])}")

    # Cluster phase
    lines.append(f"CLUSTER_PHASE {cluster_after}")

    # Pod status
    pods_list = pods_after.get("items", [])
    for pod in pods_list:
        name = pod["metadata"]["name"]
        phase = pod.get("status", {}).get("phase", "?")
        containers = pod.get("status", {}).get("containerStatuses", [])
        ready = f"{sum(1 for c in containers if c.get('ready'))}/{len(containers)}"
        restarts = {c["name"]: c.get("restartCount", 0) for c in containers}
        lines.append(
            f"POD {name} {phase} {ready} {restarts}"
        )

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = build_parser().parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"{args.root_prefix}/data/greptime/public/{{table_id}}/{{table_id}}_0000000000/"
    # TODO: this hard-codes catalog=greptime, schema=public, and region-sequence=0.
    # For labs with different catalogs/schemas or non-zero sequences, adjust the
    # prefix template or add CLI flags.

    # ---- Port-forwards ----------------------------------------------------
    pf_procs: List[Tuple[subprocess.Popen, Any]] = []
    try:
        print(f"[{now_iso()}] starting port-forwards...")
        pf_procs.append(port_forward(
            args.frontend_local_port, args.frontend_remote_port,
            args.namespace, args.frontend_service, out_dir))
        pf_procs.append(port_forward(
            args.monitor_local_port, args.monitor_remote_port,
            args.namespace, args.monitor_service, out_dir))
        pf_procs.append(port_forward(
            args.minio_local_port, args.minio_remote_port,
            args.namespace, args.minio_service, out_dir))

        ports = [args.frontend_local_port, args.monitor_local_port, args.minio_local_port]
        names = ["frontend", "monitor", "minio"]
        for name, port in zip(names, ports):
            if not wait_for_port(port):
                fail(f"Port {port} ({name}) not ready after 30s")
            print(f"  {name} port {port} ready")

        # ---- Collect baseline / table info ---------------------------------
        print(f"\n[{now_iso()}] querying table metadata...")

        # table_id
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT table_id FROM information_schema.tables "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        sql_table = json.loads(text)
        write_json(str(out_dir / "sql-table-id.json"),
                   {"sql": f"SELECT table_id FROM information_schema.tables WHERE table_name='{args.table}'",
                    "status_code": code, "response_text": text})
        rows = sql_table.get("output", [{}])[0].get("records", {}).get("rows", [])
        if not rows:
            fail(f"No table_id found for table '{args.table}'")
        table_id = int(rows[0][0])
        print(f"  table_id = {table_id}")

        # region_id (greptime_partition_id)
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT greptime_partition_id FROM information_schema.partitions "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        sql_part = json.loads(text)
        write_json(str(out_dir / "sql-region-id.json"),
                   {"sql": f"SELECT greptime_partition_id FROM information_schema.partitions WHERE table_name='{args.table}'",
                    "status_code": code, "response_text": text})
        rows = sql_part.get("output", [{}])[0].get("records", {}).get("rows", [])
        if not rows:
            fail(f"No partition_id found for table '{args.table}'")
        region_id = int(rows[0][0])
        print(f"  region_id = {region_id}")

        key_prefix = prefix.format(table_id=table_id)
        print(f"  key_prefix = {key_prefix}")

        # ---- S3 transport -------------------------------------------------
        if not args.s3_secret_key:
            fail("--s3-secret-key or GC_STRESS_S3_SECRET_KEY is required")
        transport = S3Transport(
            endpoint=args.s3_endpoint,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
        )

        # ---- Baseline S3 count --------------------------------------------
        print(f"\n[{now_iso()}] baseline S3 count...")
        baseline = count_s3_objects(transport, args.bucket, key_prefix)
        print(f"  total={baseline['total']} parquet={baseline['parquet']} manifest={baseline['manifest']}")

        # ---- Cluster baseline ----------------------------------------------
        print(f"\n[{now_iso()}] capturing cluster baseline...")
        pods_before = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-before.json"), pods_before)
        write_text(str(out_dir / "top-pods-before.txt"),
                   kubectl(["top", "pod", "-n", args.namespace]))

        if args.resume_after_compact:
            print(f"\n[{now_iso()}] resume-after-compact enabled; skipping INSERT+FLUSH and COMPACT")

            def read_elapsed(filename: str) -> float:
                try:
                    return float((out_dir / filename).read_text().strip())
                except Exception:
                    return 0.0

            insert_flush_elapsed = read_elapsed("insert-flush-elapsed-seconds.txt")
            compact_elapsed = read_elapsed("compact-elapsed-seconds.txt")
            after_flush = dict(baseline)
            after_compact = dict(baseline)
            print(f"  treating current S3 state as compacted baseline: "
                  f"total={baseline['total']} parquet={baseline['parquet']} "
                  f"manifest={baseline['manifest']}")
        else:
            # ---- INSERT + FLUSH loop --------------------------------------
            print(f"\n[{now_iso()}] starting INSERT+FLUSH loop ({args.count} iterations)...")
            end = args.start + args.count
            insert_count = 0
            flush_count = 0
            start_time = time.perf_counter()
            progress_log_path = str(out_dir / "insert-flush-progress.log")
            with open(progress_log_path, "w") as plog:
                for v in range(args.start, end):
                    val = v
                    tag = f"b{args.count // 1000}k" if args.count >= 1000 else f"b{args.count}"
                    # Match the manually validated Test B 1k/5k/10k harness exactly:
                    # insert one deterministic row, then force a flush to create SSTs.
                    sql = f"INSERT INTO {args.table} VALUES ({val}, {float(val)}, '{tag}')"
                    code, text = sql_query(args.frontend_local_port, sql, args.http_timeout)
                    if code != 200:
                        print(f"  WARNING: INSERT HTTP {code}: {text[:200]}", file=sys.stderr)
                    else:
                        insert_count += 1

                    code, text = sql_query(
                        args.frontend_local_port,
                        f"ADMIN FLUSH_TABLE('{args.table}')",
                        args.http_timeout,
                    )
                    if code != 200:
                        print(f"  WARNING: FLUSH HTTP {code}: {text[:200]}", file=sys.stderr)
                    else:
                        flush_count += 1

                    n_done = v - args.start + 1
                    if n_done % args.progress_every == 0:
                        print(f"  progress {n_done}/{args.count}")
                        plog.write(f"progress {n_done}/{args.count}\n")
                        plog.flush()

            insert_flush_elapsed = time.perf_counter() - start_time
            print(f"  done: {insert_count} INSERTs, {flush_count} FLUSHes "
                  f"in {insert_flush_elapsed:.2f}s")
            write_text(str(out_dir / "insert-flush-elapsed-seconds.txt"),
                       f"{insert_flush_elapsed}\n")

            # ---- S3 count after flush --------------------------------------
            print(f"\n[{now_iso()}] S3 count after flush...")
            after_flush = count_s3_objects(transport, args.bucket, key_prefix)
            print(f"  total={after_flush['total']} parquet={after_flush['parquet']} "
                  f"manifest={after_flush['manifest']}")

            # ---- COMPACT ---------------------------------------------------
            print(f"\n[{now_iso()}] running ADMIN COMPACT_TABLE...")
            _, compact_elapsed = timed(
                lambda: sql_query(
                    args.frontend_local_port,
                    f"ADMIN COMPACT_TABLE('{args.table}')",
                    args.http_timeout,
                ))
            print(f"  compact completed in {compact_elapsed:.2f}s")
            write_text(str(out_dir / "compact-elapsed-seconds.txt"),
                       f"{compact_elapsed}\n")

            # ---- S3 count after compact ------------------------------------
            print(f"\n[{now_iso()}] S3 count after compact...")
            after_compact = count_s3_objects(transport, args.bucket, key_prefix)
            print(f"  total={after_compact['total']} parquet={after_compact['parquet']} "
                  f"manifest={after_compact['manifest']}")

        # ---- Baseline Prometheus snapshot ----------------------------------
        print(f"\n[{now_iso()}] baseline Prometheus snapshot...")
        prom_queries = {
            "mito_gc_all": '{__name__=~"greptime_mito_gc.*"}',
            "metasrv_gc_all": '{__name__=~"greptime_metasrv_gc.*"}',
            "opendal_s3_delete_count":
                'opendal_http_request_duration_seconds_count{scheme="s3",operation="delete"}',
            "opendal_s3_delete_sum":
                'opendal_http_request_duration_seconds_sum{scheme="s3",operation="delete"}',
        }
        baseline_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "baseline-before-gc")

        # ---- FAST GC -------------------------------------------------------
        print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}) (fast mode)...")
        gc_result, gc_elapsed = timed(
            lambda: sql_query(args.frontend_local_port,
                              f"ADMIN GC_REGIONS({region_id})",
                              args.http_timeout))
        print(f"  GC completed in {gc_elapsed:.2f}s")
        write_text(str(out_dir / "gc-elapsed-seconds.txt"), f"{gc_elapsed}\n")
        code, text = gc_result
        write_json(str(out_dir / "sql-gc.json"),
                   {"sql": f"ADMIN GC_REGIONS({region_id})",
                    "status_code": code, "response_text": text})

        # ---- S3 count after GC ---------------------------------------------
        print(f"\n[{now_iso()}] S3 count after GC...")
        after_gc = count_s3_objects(transport, args.bucket, key_prefix)
        print(f"  total={after_gc['total']} parquet={after_gc['parquet']} "
              f"manifest={after_gc['manifest']}")

        # ---- Wait 60s + snapshot -------------------------------------------
        print(f"\n[{now_iso()}] waiting 60s for metrics to settle...")
        time.sleep(60)

        after_60s_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "after-gc-60s")

        # ---- SECOND GC (idempotency check) ---------------------------------
        print(f"\n[{now_iso()}] running SECOND GC_REGIONS({region_id})...")
        second_gc_result, second_gc_elapsed = timed(
            lambda: sql_query(args.frontend_local_port,
                              f"ADMIN GC_REGIONS({region_id})",
                              args.http_timeout))
        print(f"  second GC completed in {second_gc_elapsed:.2f}s")
        write_text(str(out_dir / "gc-second-elapsed-seconds.txt"),
                   f"{second_gc_elapsed}\n")
        code2, text2 = second_gc_result
        write_json(str(out_dir / "sql-gc-second.json"),
                   {"sql": f"ADMIN GC_REGIONS({region_id})",
                    "status_code": code2, "response_text": text2})

        after_second_gc = count_s3_objects(transport, args.bucket, key_prefix)
        print(f"  total={after_second_gc['total']} parquet={after_second_gc['parquet']} "
              f"manifest={after_second_gc['manifest']}")

        # ---- Cluster after --------------------------------------------------
        print(f"\n[{now_iso()}] capturing cluster after...")
        pods_after = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-after.json"), pods_after)
        write_text(str(out_dir / "top-pods-after.txt"),
                   kubectl(["top", "pod", "-n", args.namespace]))
        cluster_after = kubectl_json([
            "get", "greptimedbcluster", args.cluster_name, "-n", args.namespace
        ])
        cluster_phase = cluster_after.get("status", {}).get("clusterPhase", "Unknown")
        write_json(str(out_dir / "cluster-after.json"), cluster_after)

        # ---- Fetch logs -----------------------------------------------------
        print(f"\n[{now_iso()}] fetching pod logs (since={args.logs_since})...")
        meta_pod = kubectl_pod_name(
            f"app.greptime.io/component={args.cluster_name}-meta", args.namespace)
        meta_logs = kubectl_logs(meta_pod, args.namespace, args.logs_since)
        write_text(str(out_dir / f"meta-logs-since-{args.logs_since}.txt"), meta_logs)
        print(f"  meta logs: {len(meta_logs)} bytes")

        dn_pod = kubectl_pod_name(
            f"app.greptime.io/component={args.cluster_name}-datanode", args.namespace)
        dn_logs = kubectl_logs(dn_pod, args.namespace, args.logs_since)
        write_text(str(out_dir / f"datanode-logs-since-{args.logs_since}.txt"), dn_logs)
        print(f"  datanode logs: {len(dn_logs)} bytes")

        # ---- Parse GC report ------------------------------------------------
        print(f"\n[{now_iso()}] parsing GC reports for region {region_id}...")
        gc_reports = parse_gc_reports(meta_logs, region_id)
        print(f"  found {len(gc_reports)} GC report(s) for region {region_id}")
        write_json(str(out_dir / "gc-report-log-summary.json"),
                   {"meta_gc_report_summaries": gc_reports})

        # ---- Metrics delta --------------------------------------------------
        print(f"\n[{now_iso()}] computing metrics delta...")
        metrics_delta = compute_metrics_delta(baseline_metrics, after_60s_metrics)

        # ---- Write output files ---------------------------------------------
        object_counts = {
            "baseline": baseline,
            "after_flush": after_flush,
            "after_compact": after_compact,
            "after_gc": after_gc,
            "after_second_gc": after_second_gc,
        }
        write_json(str(out_dir / "object-counts.json"), {
            "table_id": table_id,
            "region_id": region_id,
            "prefix": key_prefix,
            "counts": object_counts,
        })

        elapsed = {
            "insert_flush": insert_flush_elapsed,
            "compact": compact_elapsed,
            "gc": gc_elapsed,
            "second_gc": second_gc_elapsed,
        }
        metrics_summary = {
            "n": args.count,
            "start": args.start,
            "table_id": table_id,
            "region_id": region_id,
            "prefix": key_prefix,
            "object_counts": object_counts,
            "insert_flush_elapsed_seconds": insert_flush_elapsed,
            "compact_elapsed_seconds": compact_elapsed,
            "gc_elapsed_seconds": gc_elapsed,
            "second_gc_elapsed_seconds": second_gc_elapsed,
            "gc_report_log_summary": gc_reports,
            "baseline_to_after_gc_60s": metrics_delta,
        }
        write_json(str(out_dir / "metrics-summary.json"), metrics_summary)

        concise = build_concise_summary(
            str(out_dir), args, table_id, region_id, key_prefix,
            object_counts, elapsed, metrics_delta, gc_reports,
            pods_after, cluster_phase,
        )
        write_text(str(out_dir / "concise-summary.txt"), concise)
        print(f"\n[{now_iso()}] all evidence written to {out_dir}/")
        print(concise)

    finally:
        # Always terminate port-forwards
        for proc, _log in pf_procs:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        for _proc, log in pf_procs:
            try:
                log.close()
            except Exception:
                pass
        print(f"\n[{now_iso()}] port-forwards terminated. Done.")


if __name__ == "__main__":
    main()
