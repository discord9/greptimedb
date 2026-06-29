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
run_active_filemeta_gc_probe.py
Test C C1 harness: active manifest/FileMeta GC probe at scale.

Generates many real SSTs via repeated INSERT + ADMIN FLUSH_TABLE,
does NOT compact, then runs fast/full GC probes expecting 0 deletions.
Captures metrics, logs, and region stats, and writes evidence to the
output directory.

Run with: uv run ...run_active_filemeta_gc_probe.py --count 1000 --start 0 --out /tmp/gc-test-c-Nk/

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
import urllib.error
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
        description="Test C: active-filemeta GC probe harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Required
    p.add_argument("--count", type=int, required=True,
                   help="Number of INSERT+FLUSH iterations (e.g. 1000)")
    p.add_argument("--start", type=int, default=0,
                   help="Starting value for INSERT (default: 0)")
    p.add_argument("--out", required=True,
                   help="Output directory for evidence files")

    # Cluster
    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--table", default="gc_hf_test_c")
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
                   help="Print progress every N INSERT statements (default: 1000)")
    g.add_argument("--stats-every", type=int, default=1000,
                   help="Sample region stats every N INSERT statements (default: 1000)")
    g.add_argument("--logs-since", default="6h",
                   help="kubectl logs --since duration (default: 6h)")
    g.add_argument("--http-timeout", type=float, default=600.0,
                   help="HTTP timeout for SQL requests in seconds (default: 600)")
    g.add_argument("--settle-seconds", type=int, default=30,
                   help="Seconds to wait after generation before probing (default: 30)")

    # Flags
    g = p.add_argument_group("flags")
    g.add_argument("--create-table", action="store_true",
                   help="Create table if absent; fail if absent and flag not provided")
    g.add_argument("--skip-fast-gc", action="store_true",
                   help="Skip the fast GC probe step")
    g.add_argument("--skip-full-gc", action="store_true",
                   help="Skip the full GC probe step")

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
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode(errors="replace")
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode(errors="replace")


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


# ---- Region stats query ---------------------------------------------------
def query_region_stats(port: int, table_id: int, timeout: float
                       ) -> Dict[str, Any]:
    """Query region_statistics for the target table_id."""
    code, text = sql_query(port, (
        f"SELECT region_id, table_id, region_number, "
        f"manifest_size, sst_num, sst_size, memtable_size "
        f"FROM information_schema.region_statistics "
        f"WHERE table_id = {table_id}"
    ), timeout)
    return {"status_code": code, "response": json.loads(text)}


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
        if f"{region_id}" not in line:
            continue
        ts = _find_field(line, "timestamp") or ""
        file_id_count = line.count("FileId(")
        byte_len = len(line)
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


# ---- Log evidence counting ------------------------------------------------
def count_log_evidence(meta_logs: str, dn_logs: str) -> Dict[str, int]:
    """Count occurrences of key patterns in meta and datanode logs."""
    patterns = {
        "gc:": r"gc:",
        "full_listing": r"full listing",
        "in_manifest_file_cnt": r"In manifest file cnt",
        "compaction": r"compaction",
        "gc_report": r"GC report",
    }
    counts: Dict[str, int] = {}
    for label, pat in patterns.items():
        meta_count = len(re.findall(pat, meta_logs, re.IGNORECASE))
        dn_count = len(re.findall(pat, dn_logs, re.IGNORECASE))
        counts[label] = meta_count + dn_count
    return counts


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
    object_counts: Dict[str, Any],
    elapsed: Dict[str, float],
    metrics_delta: Dict[str, Any],
    fast_gc_delta: Dict[str, Any],
    full_gc_delta: Dict[str, Any],
    gc_reports: List[Dict[str, Any]],
    pods_after: Any,
    cluster_after: str,
    log_evidence: Dict[str, int],
    region_stats_summary: Dict[str, Any],
    gate_status: str,
) -> str:
    """Build concise-summary.txt matching Test C conventions."""
    lines = [
        f"EVIDENCE_DIR {out_dir}",
        f"TABLE {args.table}",
        f"TABLE_ID {table_id}",
        f"REGION_ID {region_id}",
        f"PREFIX {prefix}",
        f"COUNT {args.count}",
        f"START {args.start}",
        f"GATE_STATUS {gate_status}",
        f"COUNTS {json.dumps(object_counts, sort_keys=True)}",
    ]
    for label in ["insert_flush", "fast_gc", "full_gc"]:
        e = elapsed.get(label)
        if e is not None:
            lines.append(f"{label.upper()}_ELAPSED_SECONDS {e}")

    # Region stats snapshot
    lines.append(f"REGION_STATS_SUMMARY {json.dumps(region_stats_summary, sort_keys=True)}")

    def _s3_delete_count_deltas(delta: Dict[str, Any]) -> List[Dict[str, Any]]:
        return delta.get("opendal_s3_delete_count", {}).get("changed_or_new", [])

    lines.append(
        f"FAST_GC_S3_DELETE_DELTAS {json.dumps(_s3_delete_count_deltas(fast_gc_delta), sort_keys=True)}"
    )
    lines.append(
        f"FULL_GC_S3_DELETE_DELTAS {json.dumps(_s3_delete_count_deltas(full_gc_delta), sort_keys=True)}"
    )

    # Extract key overall metrics deltas (baseline before generation -> final).
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
            f"OVERALL OpenDAL S3 delete count: "
            f"{c.get('before')} -> {c.get('after')} delta={c.get('delta')}"
        )
    od_sum = metrics_delta.get("opendal_s3_delete_sum", {}).get("changed_or_new", [])
    for c in od_sum:
        lines.append(f"OVERALL OpenDAL S3 delete duration sum delta={c.get('delta')}")

    # Log evidence
    lines.append(f"LOG_EVIDENCE {json.dumps(log_evidence, sort_keys=True)}")

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

    # ---- Port-forwards ----------------------------------------------------
    pf_procs: List[Tuple[subprocess.Popen, Any]] = []
    gate_status = "passed"
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

        # ---- S3 transport -------------------------------------------------
        if not args.s3_secret_key:
            fail("--s3-secret-key or GC_STRESS_S3_SECRET_KEY is required")
        transport = S3Transport(
            endpoint=args.s3_endpoint,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
        )

        # ---- Cluster baseline ----------------------------------------------
        print(f"\n[{now_iso()}] capturing cluster baseline...")
        pods_before = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-before.json"), pods_before)
        write_text(str(out_dir / "top-pods-before.txt"),
                   kubectl(["top", "pod", "-n", args.namespace]))
        cluster_before = kubectl_json([
            "get", "greptimedbcluster", args.cluster_name, "-n", args.namespace
        ])
        write_json(str(out_dir / "cluster-before.json"), cluster_before)

        # ---- Table setup ---------------------------------------------------
        print(f"\n[{now_iso()}] verifying table '{args.table}'...")
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT table_name FROM information_schema.tables "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        resp = json.loads(text)
        tables = (resp.get("output", [{}])[0]
                  .get("records", {}).get("rows", []))
        table_exists = len(tables) > 0

        if not table_exists:
            if args.create_table:
                ddl = (
                    f"CREATE TABLE {args.table} ("
                    f"v BIGINT, x DOUBLE, label STRING, ts TIMESTAMP TIME INDEX)"
                    f" ENGINE=mito WITH ("
                    f"'append_mode' = 'true', "
                    f"'compaction.type' = 'twcs', "
                    f"'compaction.twcs.trigger_file_num' = '1000000000')"
                )
                print(f"  table absent; creating with DDL…")
                c_code, c_text = sql_query(args.frontend_local_port, ddl, args.http_timeout)
                if c_code != 200:
                    fail(f"CREATE TABLE failed HTTP {c_code}: {c_text[:200]}")
                print(f"  table created.")
            else:
                fail(f"Table '{args.table}' not found and --create-table not set")

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

        # ---- Baseline S3 count --------------------------------------------
        print(f"\n[{now_iso()}] baseline S3 count...")
        baseline = count_s3_objects(transport, args.bucket, key_prefix)
        print(f"  total={baseline['total']} parquet={baseline['parquet']} manifest={baseline['manifest']}")
        baseline_parquet = baseline["parquet"]

        # ---- Region stats before -------------------------------------------
        print(f"\n[{now_iso()}] region stats before generation...")
        region_stats_before = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-before.json"), region_stats_before)

        # ---- Baseline Prometheus snapshot (before generation) --------------
        print(f"\n[{now_iso()}] baseline Prometheus snapshot (before generation)...")
        prom_queries = {
            "mito_gc_all": '{__name__=~"greptime_mito_gc.*"}',
            "metasrv_gc_all": '{__name__=~"greptime_metasrv_gc.*"}',
            "opendal_s3_delete_count":
                'opendal_http_request_duration_seconds_count{scheme="s3",operation="delete"}',
            "opendal_s3_delete_sum":
                'opendal_http_request_duration_seconds_sum{scheme="s3",operation="delete"}',
            "manifest_ops": '{__name__=~"greptime_manifest.*"}',
            "flush_ops": '{__name__=~"greptime_mito_flush.*"}',
            "compaction_ops": '{__name__=~"greptime_mito_compaction.*"}',
            "gc_ops": '{__name__=~"greptime_mito_gc.*"}',
            "s3_list_delete": '{__name__=~"opendal_http_request.*",scheme="s3"}',
            "memory_working_set": 'container_memory_working_set_bytes{namespace="' + args.namespace + '"}',
        }
        baseline_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "baseline-before-generate")

        # ---- INSERT + FLUSH loop ------------------------------------------
        print(f"\n[{now_iso()}] starting INSERT+FLUSH loop ({args.count} iterations)...")
        end = args.start + args.count
        insert_count = 0
        flush_count = 0
        start_time = time.perf_counter()
        progress_log_path = str(out_dir / "insert-flush-progress.log")
        samples_path = str(out_dir / "progress-samples.jsonl")
        with open(progress_log_path, "w") as plog, open(samples_path, "w") as samples_f:
            for v in range(args.start, end):
                val = v
                tag_val = f"c{args.count}"
                sql = f"INSERT INTO {args.table} VALUES ({val}, {float(val)}, '{tag_val}', now())"
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

                # Periodic region-stats + S3 sampling
                if n_done % args.stats_every == 0:
                    sample_ts = time.time()
                    rs = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
                    s3 = count_s3_objects(transport, args.bucket, key_prefix)
                    sample = {
                        "ts": now_iso(),
                        "ts_epoch": sample_ts,
                        "iteration": n_done,
                        "region_stats": rs,
                        "s3_objects": s3,
                    }
                    samples_f.write(json.dumps(sample) + "\n")
                    samples_f.flush()

        insert_flush_elapsed = time.perf_counter() - start_time
        print(f"  done: {insert_count} INSERT statements, {flush_count} FLUSH statements "
              f"in {insert_flush_elapsed:.2f}s")
        write_text(str(out_dir / "insert-flush-elapsed-seconds.txt"),
                   f"{insert_flush_elapsed}\n")

        # ---- Region stats after generation ---------------------------------
        print(f"\n[{now_iso()}] region stats after generation...")
        region_stats_after = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-after-generate.json"), region_stats_after)

        # ---- S3 count after generation ------------------------------------
        print(f"\n[{now_iso()}] S3 count after generation...")
        after_generate = count_s3_objects(transport, args.bucket, key_prefix)
        print(f"  total={after_generate['total']} parquet={after_generate['parquet']} "
              f"manifest={after_generate['manifest']}")
        write_json(str(out_dir / "object-counts.json"), {
            "table_id": table_id,
            "region_id": region_id,
            "prefix": key_prefix,
            "baseline": baseline,
            "after_generate": after_generate,
        })

        # ---- Safety: warn if parquet count decreased from baseline ----------
        if table_exists and after_generate["parquet"] < baseline_parquet:
            msg = (
                f"WARNING: after-generation parquet count ({after_generate['parquet']}) "
                f"is lower than baseline ({baseline_parquet}) for reused table. "
                f"This may indicate data loss, but will NOT delete any S3 objects."
            )
            print(f"\n[{now_iso()}] {msg}", file=sys.stderr)
            write_text(str(out_dir / "parquet-count-decrease-warning.txt"), msg + "\n")

        # ---- Prometheus snapshot after generation --------------------------
        print(f"\n[{now_iso()}] Prometheus snapshot after generation...")
        after_gen_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "after-generate")

        # ---- Settle --------------------------------------------------------
        print(f"\n[{now_iso()}] settling for {args.settle_seconds}s...")
        time.sleep(args.settle_seconds)

        # ---- Pods after generation -----------------------------------------
        pods_after_gen = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-after-generate.json"), pods_after_gen)

        # ---- FAST GC probe -------------------------------------------------
        fast_gc_elapsed = 0.0
        fast_delta: Dict[str, Any] = {}
        after_fast_gc: Dict[str, int] = dict(after_generate)
        if args.skip_fast_gc:
            print(f"\n[{now_iso()}] skipping fast GC probe (--skip-fast-gc)")
        else:
            print(f"\n[{now_iso()}] prometheus snapshot before fast GC...")
            fast_gc_before_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir),
                "before-fast-gc")

            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}) (fast mode)...")
            gc_result, fast_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id})",
                                  args.http_timeout))
            print(f"  fast GC completed in {fast_gc_elapsed:.2f}s")
            write_text(str(out_dir / "fast-gc-elapsed-seconds.txt"),
                       f"{fast_gc_elapsed}\n")
            code, text = gc_result
            write_json(str(out_dir / "sql-fast-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id})",
                        "status_code": code, "response_text": text})

            # ---- S3 count after fast GC -----------------------------------
            print(f"\n[{now_iso()}] S3 count after fast GC...")
            after_fast_gc = count_s3_objects(transport, args.bucket, key_prefix)
            print(f"  total={after_fast_gc['total']} parquet={after_fast_gc['parquet']} "
                  f"manifest={after_fast_gc['manifest']}")

            # Update object counts
            current_counts = json.loads((out_dir / "object-counts.json").read_text())
            current_counts["after_fast_gc"] = after_fast_gc
            write_json(str(out_dir / "object-counts.json"), current_counts)

            # ---- Prometheus snapshot after fast GC -------------------------
            print(f"\n[{now_iso()}] prometheus snapshot after fast GC...")
            fast_gc_after_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir),
                "after-fast-gc")

            # ---- Fast GC safety check --------------------------------------
            fast_delta = compute_metrics_delta(fast_gc_before_metrics, fast_gc_after_metrics)
            write_json(str(out_dir / "metrics-delta-fast-gc.json"), fast_delta)

            if after_fast_gc["parquet"] < after_generate["parquet"]:
                print(f"\n[{now_iso()}] FAIL: parquet count decreased after fast GC "
                      f"({after_generate['parquet']} -> {after_fast_gc['parquet']})",
                      file=sys.stderr)
                gate_status = "failed"

            # Check S3 delete metrics delta
            od_count_fast = (fast_delta.get("opendal_s3_delete_count", {})
                             .get("changed_or_new", []))
            for c in od_count_fast:
                if c.get("delta", 0) > 0:
                    print(f"\n[{now_iso()}] FAIL: positive S3 delete metric delta "
                          f"after fast GC: {c}", file=sys.stderr)
                    gate_status = "failed"

        # ---- Region stats after fast GC ------------------------------------
        region_stats_after_fast = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-after-fast-gc.json"), region_stats_after_fast)

        # ---- FULL GC probe -------------------------------------------------
        full_gc_elapsed = 0.0
        full_delta: Dict[str, Any] = {}
        after_full_gc: Dict[str, int] = dict(after_fast_gc)
        if args.skip_full_gc:
            print(f"\n[{now_iso()}] skipping full GC probe (--skip-full-gc)")
        else:
            print(f"\n[{now_iso()}] prometheus snapshot before full GC...")
            full_gc_before_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir),
                "before-full-gc")

            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}, true) (full mode)...")
            full_gc_result, full_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id}, true)",
                                  args.http_timeout))
            print(f"  full GC completed in {full_gc_elapsed:.2f}s")
            write_text(str(out_dir / "full-gc-elapsed-seconds.txt"),
                       f"{full_gc_elapsed}\n")
            code2, text2 = full_gc_result
            write_json(str(out_dir / "sql-full-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id}, true)",
                        "status_code": code2, "response_text": text2})

            # ---- S3 count after full GC -----------------------------------
            print(f"\n[{now_iso()}] S3 count after full GC...")
            after_full_gc = count_s3_objects(transport, args.bucket, key_prefix)
            print(f"  total={after_full_gc['total']} parquet={after_full_gc['parquet']} "
                  f"manifest={after_full_gc['manifest']}")

            # Update object counts
            current_counts = json.loads((out_dir / "object-counts.json").read_text())
            current_counts["after_full_gc"] = after_full_gc
            write_json(str(out_dir / "object-counts.json"), current_counts)

            # ---- Prometheus snapshot after full GC -------------------------
            print(f"\n[{now_iso()}] prometheus snapshot after full GC...")
            full_gc_after_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir),
                "after-full-gc")

            # ---- Full GC safety check --------------------------------------
            full_delta = compute_metrics_delta(full_gc_before_metrics, full_gc_after_metrics)
            write_json(str(out_dir / "metrics-delta-full-gc.json"), full_delta)

            comparison_base = after_generate if args.skip_fast_gc else after_fast_gc
            if after_full_gc["parquet"] < comparison_base["parquet"]:
                print(f"\n[{now_iso()}] FAIL: parquet count decreased after full GC "
                      f"({comparison_base['parquet']} -> {after_full_gc['parquet']})",
                      file=sys.stderr)
                gate_status = "failed"

            # Check S3 delete metrics delta
            od_count_full = (full_delta.get("opendal_s3_delete_count", {})
                             .get("changed_or_new", []))
            for c in od_count_full:
                if c.get("delta", 0) > 0:
                    print(f"\n[{now_iso()}] FAIL: positive S3 delete metric delta "
                          f"after full GC: {c}", file=sys.stderr)
                    gate_status = "failed"

        # ---- Region stats after full GC ------------------------------------
        region_stats_after_full = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-after-full-gc.json"), region_stats_after_full)

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

        # ---- Cluster phase / pod restart safety -----------------------------
        if cluster_phase != "Running":
            print(f"\n[{now_iso()}] FAIL: cluster phase is '{cluster_phase}', not Running",
                  file=sys.stderr)
            gate_status = "failed"

        # Check pod restarts increased
        pods_before_items = pods_before.get("items", [])
        pods_after_items = pods_after.get("items", [])
        for after_pod in pods_after_items:
            after_name = after_pod["metadata"]["name"]
            after_containers = after_pod.get("status", {}).get("containerStatuses", [])
            before_pod = next((p for p in pods_before_items
                               if p["metadata"]["name"] == after_name), None)
            if before_pod is None:
                continue
            before_containers = before_pod.get("status", {}).get("containerStatuses", [])
            before_restarts = {c["name"]: c.get("restartCount", 0) for c in before_containers}
            for c in after_containers:
                if c.get("restartCount", 0) > before_restarts.get(c["name"], 0):
                    print(f"\n[{now_iso()}] FAIL: pod {after_name} container {c['name']} "
                          f"restart count increased", file=sys.stderr)
                    gate_status = "failed"

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

        # ---- Log evidence counts --------------------------------------------
        print(f"\n[{now_iso()}] counting log evidence patterns...")
        log_evidence = count_log_evidence(meta_logs, dn_logs)
        print(f"  log_evidence = {json.dumps(log_evidence)}")

        # ---- Metrics delta (overall: baseline -> after full-gc compare) -----
        print(f"\n[{now_iso()}] computing overall metrics delta...")
        # Snapshot final metrics for overall delta
        final_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "final")
        metrics_delta = compute_metrics_delta(baseline_metrics, final_metrics)

        # ---- Region stats summary -------------------------------------------
        def _extract_first_row(resp: Dict) -> Dict[str, Any]:
            rows = (resp.get("response", {}).get("output", [{}])[0]
                    .get("records", {}).get("rows", []))
            if rows:
                cols = ["region_id", "table_id", "region_number",
                        "manifest_size", "sst_num", "sst_size", "memtable_size"]
                return dict(zip(cols, rows[0]))
            return {}

        region_stats_summary = {
            "before": _extract_first_row(region_stats_before),
            "after_generate": _extract_first_row(region_stats_after),
            "after_fast_gc": _extract_first_row(region_stats_after_fast),
            "after_full_gc": _extract_first_row(region_stats_after_full),
        }

        # ---- Write output files ---------------------------------------------
        object_counts = {
            "baseline": baseline,
            "after_generate": after_generate,
            "after_fast_gc": after_fast_gc,
            "after_full_gc": after_full_gc,
        }

        elapsed = {
            "insert_flush": insert_flush_elapsed,
            "fast_gc": fast_gc_elapsed,
            "full_gc": full_gc_elapsed,
        }
        metrics_summary = {
            "n": args.count,
            "start": args.start,
            "table_id": table_id,
            "region_id": region_id,
            "prefix": key_prefix,
            "object_counts": object_counts,
            "insert_flush_elapsed_seconds": insert_flush_elapsed,
            "fast_gc_elapsed_seconds": fast_gc_elapsed,
            "full_gc_elapsed_seconds": full_gc_elapsed,
            "gc_report_log_summary": gc_reports,
            "log_evidence": log_evidence,
            "region_stats_summary": region_stats_summary,
            "baseline_to_final": metrics_delta,
            "fast_gc_delta": fast_delta,
            "full_gc_delta": full_delta,
            "gate_status": gate_status,
        }
        write_json(str(out_dir / "metrics-summary.json"), metrics_summary)

        concise = build_concise_summary(
            str(out_dir), args, table_id, region_id, key_prefix,
            object_counts, elapsed, metrics_delta, fast_delta, full_delta, gc_reports,
            pods_after, cluster_phase, log_evidence, region_stats_summary,
            gate_status,
        )
        write_text(str(out_dir / "concise-summary.txt"), concise)
        print(f"\n[{now_iso()}] all evidence written to {out_dir}/")
        print(concise)

        if gate_status == "failed":
            print(f"\n[{now_iso()}] gate_status=failed; exiting with code 1")
            sys.exit(1)

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
