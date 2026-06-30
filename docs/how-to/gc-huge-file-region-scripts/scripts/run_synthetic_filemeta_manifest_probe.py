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
run_synthetic_filemeta_manifest_probe.py
Test C2 harness: synthetic checkpoint manifest (checkpoint or delta-only) probe.
C2b extension: optional active-object materialization (placeholder .parquet).

Flow:
1. Create seed table + insert one row → flush to generate a real manifest checkpoint.
2. Backup existing manifest objects from S3.
3. Download seed checkpoint (or delta JSON files), run Rust generator locally to
   produce synthetic manifest with `count` active FileMeta entries.
4. [C2b opt-in] Write tiny placeholder .parquet objects matching every generated
   active FileMeta (NOT readable SSTs).  These exercise the GC full-listing/filter
   path and the active-known deletion guard.
5. Scale datanode to 0, PUT generated checkpoint + _last_checkpoint, scale back.
6. Verify region_statistics.sst_num, run fast/full GC probes expecting 0 deletes,
   capture evidence, and write concise summary.

Seed can be a checkpoint or delta-only files:
- If _last_checkpoint exists: download checkpoint as seed.
- If _last_checkpoint missing (404/NoSuchKey): replay uncompressed .json deltas
  from the backed-up manifest objects. .json.gz deltas are rejected.

Dry-run (no --execute): prints plan and exits.

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
        description="Test C2: synthetic-filemeta-manifest probe harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Required
    p.add_argument("--count", type=int, required=True,
                   help="Number of synthetic FileMeta entries (e.g. 1000)")
    p.add_argument("--out", required=True,
                   help="Output directory for evidence files")

    # Cluster
    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--table", default="gc_hf_test_c2")
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
    g = p.add_argument_group("s3")
    g.add_argument("--s3-endpoint", default="http://127.0.0.1:19000")
    g.add_argument("--s3-access-key", default=os.environ.get("GC_STRESS_S3_ACCESS_KEY", "rootuser"))
    g.add_argument("--s3-secret-key", default=os.environ.get("GC_STRESS_S3_SECRET_KEY", ""),
                   help="S3 secret key, or set GC_STRESS_S3_SECRET_KEY")
    g.add_argument("--bucket", default="gc-stress-bucket")
    g.add_argument("--root-prefix", default="gc-hf-lab")
    g.add_argument("--storage-path", default="greptime/public",
                   help="Path component inside root-prefix (default: greptime/public)")

    # Rate
    g = p.add_argument_group("rate")
    g.add_argument("--logs-since", default="6h")
    g.add_argument("--http-timeout", type=float, default=600.0)
    g.add_argument("--settle-seconds", type=int, default=30)

    # Flags
    g = p.add_argument_group("flags")
    g.add_argument("--execute", action="store_true",
                   help="Execute the full flow. Without this, dry-run only.")
    g.add_argument("--create-table", action="store_true",
                   help="Create table if absent; fail if absent and flag not provided")
    g.add_argument("--allow-large", action="store_true",
                   help="Forward --allow-large to Rust generator for count > 100k")
    g.add_argument("--skip-fast-gc", action="store_true")
    g.add_argument("--skip-full-gc", action="store_true")
    g.add_argument("--forbid-table-id", type=int, action="append", default=None,
                   help="Forbid a table_id (repeatable; default includes 1035)")
    g.add_argument("--checkpoint-version", type=int, default=1,
                   help="Checkpoint version to use (default: 1)")
    g.add_argument("--generator-bin", default=None,
                   help="Path or command for Rust generator "
                        "(default: cargo run -p cmd --bin gc_synthetic_manifest --)")

    # C2b: object materialization (placeholder .parquet matching manifest)
    g = p.add_argument_group("c2b-materialize")
    g.add_argument("--materialize-active-objects", action="store_true", default=False,
                   help="Write tiny placeholder .parquet objects for every generated "
                        "FileMeta (C2b active-object protection test). "
                        "These are NOT readable SSTs; only used for full-listing/filter "
                        "and active-known protection testing.")
    g.add_argument("--placeholder-object-bytes", type=int, default=1,
                   help="Size in bytes of each placeholder .parquet object (default: 1)")
    g.add_argument("--max-materialize-count", type=int, default=1000,
                   help="Safety cap: refuse to materialize if generated count exceeds "
                        "this value unless --allow-large is also set (default: 1000)")

    return p


# ---------------------------------------------------------------------------
# Helpers (shared style from run_active_filemeta_gc_probe.py)
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


def kubectl_pod_name_optional(label: str, namespace: str) -> Optional[str]:
    pod_list = kubectl_json(["get", "pod", "-n", namespace, "-l", label])
    items = pod_list.get("items", [])
    if not items:
        return None
    return items[0].get("metadata", {}).get("name", "") or None


def kubectl_pod_name(label: str, namespace: str) -> str:
    name = kubectl_pod_name_optional(label, namespace)
    if not name:
        fail(f"No pod found for label '{label}' in namespace '{namespace}'")
    assert name is not None
    return name


# ---- S3 helpers -----------------------------------------------------------
def s3_get_object(transport: S3Transport, bucket: str, key: str) -> bytes:
    """Download an object from S3. Returns raw bytes."""
    try:
        _, data = transport._request("GET", bucket, key)
        return data
    except RuntimeError as e:
        fail(f"S3 GET {bucket}/{key} failed: {e}")
        raise  # unreachable, satisfies type checker


def s3_get_object_optional(transport: S3Transport, bucket: str, key: str
                           ) -> Optional[bytes]:
    """Download an object from S3. Returns None on 404, raises on other errors."""
    try:
        _, data = transport._request("GET", bucket, key)
        return data
    except RuntimeError as e:
        msg = str(e)
        if "HTTP 404" in msg or "NoSuchKey" in msg:
            return None
        raise


def count_s3_objects(transport: S3Transport, bucket: str, prefix: str
                     ) -> Dict[str, int]:
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


def list_s3_objects(transport: S3Transport, bucket: str, prefix: str
                    ) -> List[Dict[str, Any]]:
    """List all objects under prefix. Returns list of {key, size}."""
    result = []
    for key, size in transport.list_objects_v2(bucket, prefix):
        result.append({"key": key, "size": size})
    return result


def max_manifest_object_version(objects: List[Dict[str, Any]]) -> int:
    """Return max manifest file version found in checkpoint/delta object names."""
    max_version = 0
    for obj in objects:
        name = os.path.basename(obj["key"])
        m = re.match(r"^(\d{20})\.(?:checkpoint|json)(?:\.gz)?$", name)
        if m:
            max_version = max(max_version, int(m.group(1)))
    return max_version


# ---- Region stats ---------------------------------------------------------
def query_region_stats(port: int, table_id: int, timeout: float
                       ) -> Dict[str, Any]:
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


def count_log_evidence(meta_logs: str, dn_logs: str) -> Dict[str, int]:
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
    gc_reports: List[Dict[str, Any]],
    pods_after: Any,
    cluster_after: str,
    log_evidence: Dict[str, int],
    region_stats_summary: Dict[str, Any],
    gate_status: str,
    sst_num_expected: int,
) -> str:
    lines = [
        f"EVIDENCE_DIR {out_dir}",
        f"TABLE {args.table}",
        f"TABLE_ID {table_id}",
        f"REGION_ID {region_id}",
        f"PREFIX {prefix}",
        f"COUNT {args.count}",
        f"SST_NUM_EXPECTED {sst_num_expected}",
        f"GATE_STATUS {gate_status}",
        f"COUNTS {json.dumps(object_counts, sort_keys=True)}",
    ]
    for label in ["seed_flush", "generator", "datanode_scale", "fast_gc", "full_gc"]:
        e = elapsed.get(label)
        if e is not None:
            lines.append(f"{label.upper()}_ELAPSED_SECONDS {e}")

    lines.append(f"REGION_STATS_SUMMARY {json.dumps(region_stats_summary, sort_keys=True)}")

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

    od_count = metrics_delta.get("opendal_s3_delete_count", {}).get("changed_or_new", [])
    for c in od_count:
        lines.append(
            f"OpenDAL S3 delete count: "
            f"{c.get('before')} -> {c.get('after')} delta={c.get('delta')}"
        )

    lines.append(f"LOG_EVIDENCE {json.dumps(log_evidence, sort_keys=True)}")
    lines.append(f"GC_REPORT_LOG_SUMMARY {json.dumps(gc_reports[-8:])}")
    lines.append(f"CLUSTER_PHASE {cluster_after}")

    pods_list = pods_after.get("items", [])
    for pod in pods_list:
        name = pod["metadata"]["name"]
        phase = pod.get("status", {}).get("phase", "?")
        containers = pod.get("status", {}).get("containerStatuses", [])
        ready = f"{sum(1 for c in containers if c.get('ready'))}/{len(containers)}"
        restarts = {c["name"]: c.get("restartCount", 0) for c in containers}
        lines.append(f"POD {name} {phase} {ready} {restarts}")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Dry-run plan
# ---------------------------------------------------------------------------
def dry_run_plan(args: argparse.Namespace) -> None:
    print("=== DRY-RUN PLAN ===")
    print(f"Mode:          dry-run (no cluster/object-store writes)")
    print(f"Table:         {args.table}")
    print(f"Namespace:     {args.namespace}")
    print(f"Count:         {args.count}")
    print(f"Out dir:       {args.out}")
    print(f"Execute:       {args.execute}")
    print(f"Create table:  {args.create_table}")
    print(f"Checkpoint ver:{args.checkpoint_version}")
    print(f"Forbid IDs:    {args.forbid_table_id or [1035]}")
    print()
    print("Planned steps:")
    print("  1. port-forward frontend/monitor/minio")
    print("  2. create seed table (if --create-table)")
    print("  3. insert 1 row + flush → generate real manifest checkpoint")
    print("  4. query table_id, region_id; validate against forbid list")
    print("  5. list + backup manifest S3 objects")
    print("  6. find seed: checkpoint (if _last_checkpoint exists) or delta-only")
    print("  7. download seed checkpoint (checkpoint path) or prepare delta dir")
    print("  8. run Rust generator (dry-run then real)")
    print("  9. baseline cluster capture")
    print(" 10. scale datanode → 0")
    print(" 10b.[if --materialize-active-objects] write placeholder .parquet objects")
    print(" 11. PUT generated checkpoint + _last_checkpoint")
    print(" 12. scale datanode → 1, wait Ready")
    print(" 13. verify region_statistics.sst_num")
    print(" 14. fast/full GC probes (expect 0 deletes)")
    print(" 14b.[if materialized] verify active objects not deleted by full GC")
    print(" 15. write evidence + concise summary")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = build_parser().parse_args()

    # Default forbid list: protect C1 table (default table_id = 1035 in our lab)
    if args.forbid_table_id is None:
        args.forbid_table_id = [1035]
    # Also forbid the C1 table by name
    forbid_table_names = {"gc_hf_test_c"}

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.execute:
        dry_run_plan(args)
        print(f"\n[{now_iso()}] Dry-run complete. Use --execute to run.")
        return

    # ======================================================================
    # EXECUTE MODE
    # ======================================================================
    gate_status = "passed"
    elapsed: Dict[str, float] = {}
    sts_name = f"{args.cluster_name}-datanode"
    datanode_scaled_down = False

    pf_procs: List[Tuple[subprocess.Popen, Any]] = []
    try:
        # ---- 1. Port-forwards --------------------------------------------
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

        # ---- S3 transport ------------------------------------------------
        if not args.s3_secret_key:
            fail("--s3-secret-key or GC_STRESS_S3_SECRET_KEY is required")
        transport = S3Transport(
            endpoint=args.s3_endpoint,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
        )

        # ---- 2. Create table ---------------------------------------------
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
                print(f"  table absent; creating...")
                c_code, c_text = sql_query(args.frontend_local_port, ddl, args.http_timeout)
                if c_code != 200:
                    fail(f"CREATE TABLE failed HTTP {c_code}: {c_text[:200]}")
                print(f"  table created.")
            else:
                fail(f"Table '{args.table}' not found and --create-table not set")

        # ---- 3. Query metadata -------------------------------------------
        print(f"\n[{now_iso()}] querying table metadata...")
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT table_id FROM information_schema.tables "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        sql_table = json.loads(text)
        rows = sql_table.get("output", [{}])[0].get("records", {}).get("rows", [])
        if not rows:
            fail(f"No table_id found for table '{args.table}'")
        table_id = int(rows[0][0])
        print(f"  table_id = {table_id}")

        # Safety: check forbidden table IDs and names
        if table_id in (args.forbid_table_id or []):
            fail(f"table_id={table_id} is in forbidden list {args.forbid_table_id}")
        if args.table in forbid_table_names:
            fail(f"table name '{args.table}' is forbidden (protected C1 table)")

        code, text = sql_query(args.frontend_local_port,
                               f"SELECT greptime_partition_id FROM information_schema.partitions "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        sql_part = json.loads(text)
        rows = sql_part.get("output", [{}])[0].get("records", {}).get("rows", [])
        if not rows:
            fail(f"No partition_id found for table '{args.table}'")
        region_id = int(rows[0][0])
        print(f"  region_id = {region_id}")

        # Compute prefixes
        region_prefix = (
            f"{args.root_prefix}/data/{args.storage_path}/"
            f"{table_id}/{table_id}_0000000000/"
        )
        manifest_prefix = region_prefix + "manifest/"
        print(f"  region_prefix  = {region_prefix}")
        print(f"  manifest_prefix = {manifest_prefix}")

        # Guard: prefix must contain gc-hf-lab
        if "gc-hf-lab" not in region_prefix:
            fail(f"region_prefix does not contain 'gc-hf-lab': {region_prefix}")

        # ---- 4. Seed row + flush (idempotent) ----------------------------
        print(f"\n[{now_iso()}] inserting seed row + flush...")
        seed_value = int(time.time() * 1000) % 1000000000
        seed_sql = (
            f"INSERT INTO {args.table} VALUES "
            f"({seed_value}, {float(seed_value)}, 'c2_seed', now())"
        )
        code, text = sql_query(args.frontend_local_port, seed_sql, args.http_timeout)
        if code != 200:
            print(f"  WARNING: seed INSERT HTTP {code}: {text[:200]}", file=sys.stderr)
        code, text = sql_query(
            args.frontend_local_port,
            f"ADMIN FLUSH_TABLE('{args.table}')",
            args.http_timeout,
        )
        if code != 200:
            print(f"  WARNING: seed FLUSH HTTP {code}: {text[:200]}", file=sys.stderr)
        seed_flush_elapsed = 0.0  # approximate
        elapsed["seed_flush"] = seed_flush_elapsed
        print(f"  seed row inserted + flushed")

        # ---- 5. S3 manifest backup ---------------------------------------
        print(f"\n[{now_iso()}] listing manifest objects...")
        manifest_objects = list_s3_objects(transport, args.bucket, manifest_prefix)
        write_json(str(out_dir / "manifest-objects-before.json"), manifest_objects)
        print(f"  found {len(manifest_objects)} manifest object(s)")
        max_existing_manifest_version = max_manifest_object_version(manifest_objects)
        target_checkpoint_version = max(
            args.checkpoint_version,
            max_existing_manifest_version,
        )
        if target_checkpoint_version != args.checkpoint_version:
            print(
                f"  using checkpoint version {target_checkpoint_version} "
                f"to cover existing manifest versions up to {max_existing_manifest_version}"
            )

        backup_dir = out_dir / "manifest-backup"
        backup_dir.mkdir(parents=True, exist_ok=True)
        for obj in manifest_objects:
            relative_key = obj["key"]
            if relative_key.startswith(manifest_prefix):
                rel_path = relative_key[len(manifest_prefix):]
            else:
                rel_path = os.path.basename(relative_key)
            local_path = backup_dir / rel_path
            local_path.parent.mkdir(parents=True, exist_ok=True)
            print(f"  backing up: {relative_key} → {local_path}")
            data = s3_get_object(transport, args.bucket, relative_key)
            local_path.write_bytes(data)
        print(f"  backup complete: {len(manifest_objects)} files")

        # ---- 6. Find seed (checkpoint or delta-only) ----------------------
        # Seed can be a checkpoint or delta-only files.  We try the normal
        # _last_checkpoint path first; if it is missing (404 / NoSuchKey),
        # we fall back to delta-only seed using uncompressed .json deltas
        # that were already backed up in step 5.
        print(f"\n[{now_iso()}] finding seed (checkpoint or delta-only)...")
        last_ck_path = manifest_prefix + "_last_checkpoint"
        last_ck_data = s3_get_object_optional(transport, args.bucket, last_ck_path)

        use_delta_seed = False
        seed_delta_dir: Optional[Path] = None
        seed_local: Optional[Path] = None
        seed_version: int = 0

        if last_ck_data is not None:
            # Normal path: _last_checkpoint exists → use checkpoint seed.
            last_ck = json.loads(last_ck_data.decode("utf-8"))
            seed_version = last_ck["version"]
            print(f"  _last_checkpoint version = {seed_version}")

            # Check for uncompressed checkpoint first
            ck_key = manifest_prefix + f"{seed_version:020}.checkpoint"
            ck_gz_key = manifest_prefix + f"{seed_version:020}.checkpoint.gz"

            seed_ck_key: str = ""
            manifest_keys = {obj["key"] for obj in manifest_objects}
            if ck_key in manifest_keys:
                seed_ck_key = ck_key
                print(f"  using uncompressed checkpoint: {ck_key}")
            elif ck_gz_key in manifest_keys:
                fail(
                    f"Only .checkpoint.gz found ({ck_gz_key}). "
                    f"gzip support not yet implemented. "
                    f"Please re-create the seed table with uncompressed checkpoints."
                )
            else:
                fail(f"Neither {ck_key} nor {ck_gz_key} found in manifest objects")

            # ---- 7. Download seed checkpoint (checkpoint path) -----------
            print(f"[{now_iso()}] downloading seed checkpoint...")
            seed_bytes = s3_get_object(transport, args.bucket, seed_ck_key)
            seed_local = out_dir / "seed.checkpoint"
            seed_local.write_bytes(seed_bytes)
            print(f"  downloaded {len(seed_bytes)} bytes → {seed_local}")

        else:
            # Delta-only path: _last_checkpoint missing → replay deltas.
            print(f"  _last_checkpoint not found (404/NoSuchKey) → delta-only seed")
            use_delta_seed = True

            # Collect uncompressed .json deltas from the backup dir.
            # Reject .json.gz files.
            delta_files: List[Path] = []
            for item in sorted(backup_dir.iterdir()):
                if not item.is_file():
                    continue
                name = item.name
                if name.endswith(".json.gz"):
                    fail(
                        f"gzip-compressed delta found ({name}) — "
                        f".json.gz not supported. "
                        f"Only uncompressed .json delta files are accepted."
                    )
                if name.endswith(".json"):
                    delta_files.append(item)

            if not delta_files:
                fail(
                    f"No uncompressed .json delta files found in {backup_dir}. "
                    f"Cannot construct seed manifest without deltas."
                )

            print(f"  using {len(delta_files)} delta file(s) from backup:")
            for df in delta_files:
                print(f"    {df.name}")
            seed_delta_dir = backup_dir

        # ---- 8. Run Rust generator ---------------------------------------
        print(f"\n[{now_iso()}] running Rust generator...")
        generated_dir = out_dir / "generated"
        generated_dir.mkdir(parents=True, exist_ok=True)

        if args.generator_bin:
            gen_cmd_prefix = args.generator_bin.split()
        else:
            gen_cmd_prefix = [
                "cargo", "run", "-p", "cmd", "--bin", "gc_synthetic_manifest", "--"
            ]

        gen_args: List[str] = [
            "--out-dir", str(generated_dir),
            "--count", str(args.count),
            "--region-id", str(region_id),
            "--version", str(target_checkpoint_version),
        ]

        if use_delta_seed:
            gen_args.extend(["--seed-delta-dir", str(seed_delta_dir)])
        else:
            gen_args.extend(["--seed-checkpoint", str(seed_local)])

        if args.allow_large:
            gen_args.append("--allow-large")

        # Run dry-run first
        print(f"  dry-run...")
        dry_cmd = gen_cmd_prefix + gen_args + ["--dry-run"]
        dry_result = subprocess.run(dry_cmd, capture_output=True, text=True,
                                    cwd=os.path.dirname(_SCRIPT_DIR))
        write_text(str(out_dir / "generator-dry-run-stdout.txt"), dry_result.stdout)
        write_text(str(out_dir / "generator-dry-run-stderr.txt"), dry_result.stderr)
        print(f"  dry-run stdout:\n{dry_result.stdout}")
        if dry_result.returncode != 0:
            fail(f"Generator dry-run failed (exit {dry_result.returncode}):\n{dry_result.stderr}")

        # Run real
        print(f"  generating...")
        gen_start = time.perf_counter()
        gen_result = subprocess.run(
            gen_cmd_prefix + gen_args,
            capture_output=True, text=True,
            cwd=os.path.dirname(_SCRIPT_DIR),
        )
        gen_elapsed = time.perf_counter() - gen_start
        elapsed["generator"] = gen_elapsed
        write_text(str(out_dir / "generator-stdout.txt"), gen_result.stdout)
        write_text(str(out_dir / "generator-stderr.txt"), gen_result.stderr)
        if gen_result.returncode != 0:
            fail(f"Generator failed (exit {gen_result.returncode}):\n{gen_result.stderr}")
        print(f"  generator completed in {gen_elapsed:.2f}s")

        # Read generator summary
        gen_summary_path = generated_dir / "summary.json"
        if gen_summary_path.exists():
            gen_summary = json.loads(gen_summary_path.read_text())
            write_json(str(out_dir / "generated-summary.json"), gen_summary)
            print(f"  generated files: {gen_summary.get('total_files_after', '?')}")

        # ---- 9. Baseline cluster capture ---------------------------------
        print(f"\n[{now_iso()}] capturing cluster baseline...")
        pods_before = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-before.json"), pods_before)
        write_text(str(out_dir / "top-pods-before.txt"),
                   kubectl(["top", "pod", "-n", args.namespace]))
        cluster_before = kubectl_json([
            "get", "greptimedbcluster", args.cluster_name, "-n", args.namespace
        ])
        write_json(str(out_dir / "cluster-before.json"), cluster_before)

        # ---- 10. Scale datanode to 0 -------------------------------------
        print(f"\n[{now_iso()}] scaling datanode StatefulSet to 0 (LAB-ONLY)...")
        subprocess.run([
            "kubectl", "scale", "statefulset", sts_name,
            "-n", args.namespace, "--replicas=0",
        ], check=True)
        datanode_scaled_down = True
        print(f"  scaled {sts_name} → 0")

        # Wait for pod to be gone
        print(f"  waiting for datanode pod to terminate...")
        label = f"app.greptime.io/component={args.cluster_name}-datanode"
        deadline = time.time() + 120
        while time.time() < deadline:
            if kubectl_pod_name_optional(label, args.namespace):
                time.sleep(2)
            else:
                # pod not found = success
                break
        else:
            fail("datanode pod did not terminate within 120s")
        print(f"  datanode offline")
        scale_down_elapsed = time.perf_counter() - (time.perf_counter() - 0)  # placeholder

        # ---- 10.5 Materialize active placeholder objects (C2b) -------------
        # C2b: write tiny placeholder .parquet objects whose paths match the
        # generated active FileMeta entries.  These are NOT readable SSTs;
        # they only exist so that GC full listing sees them as active region
        # files and exercises the active-known protection path.
        materialized_parquet_count = 0
        pre_materialize_parquet_count = 0
        if args.materialize_active_objects:
            print(f"\n[{now_iso()}] materializing active placeholder .parquet objects...")

            # Safety caps: only with --execute, count must not exceed cap
            # unless --allow-large is explicitly set.
            if args.count > args.max_materialize_count and not args.allow_large:
                fail(
                    f"Refusing to materialize {args.count} objects: exceeds "
                    f"--max-materialize-count {args.max_materialize_count}. "
                    f"Use --allow-large to override."
                )

            # Read files.jsonl from generator output
            files_jsonl = generated_dir / "files.jsonl"
            if not files_jsonl.exists():
                fail(f"files.jsonl not found in generated dir: {files_jsonl}")

            # Count existing parquet objects before materialization
            pre_obj_counts = count_s3_objects(transport, args.bucket, region_prefix)
            write_json(str(out_dir / "s3-counts-before-materialize.json"), pre_obj_counts)
            pre_parquet = pre_obj_counts["parquet"]
            pre_materialize_parquet_count = pre_parquet
            print(f"  parquet before materialize: {pre_parquet}")

            # Write placeholder objects
            placeholder = bytes(args.placeholder_object_bytes)
            written = 0
            t0 = time.perf_counter()
            with open(files_jsonl, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    ref = json.loads(line)
                    file_id = ref["file_id"]
                    parquet_key = f"{region_prefix}{file_id}.parquet"
                    transport.put_object(args.bucket, parquet_key, placeholder)
                    written += 1
                    if written % 100 == 0:
                        print(f"  materialized {written}/{args.count} objects...")

            mat_elapsed = time.perf_counter() - t0
            elapsed["materialize_objects"] = mat_elapsed
            materialized_parquet_count = written
            print(f"  materialized {written} placeholder objects in {mat_elapsed:.2f}s")

            # Count after materialization
            post_obj_counts = count_s3_objects(transport, args.bucket, region_prefix)
            write_json(str(out_dir / "s3-counts-after-materialize.json"), post_obj_counts)
            post_parquet = post_obj_counts["parquet"]
            print(f"  parquet after materialize: {post_parquet} (delta: {post_parquet - pre_parquet})")

            if post_parquet - pre_parquet != args.count:
                print(
                    f"WARNING: expected {args.count} new parquet objects, "
                    f"got {post_parquet - pre_parquet}",
                    file=sys.stderr,
                )

        # ---- 11. PUT generated checkpoint + _last_checkpoint -------------
        print(f"\n[{now_iso()}] PUTting generated checkpoint...")
        ck_file = generated_dir / f"{target_checkpoint_version:020}.checkpoint"
        if not ck_file.exists():
            fail(f"Generated checkpoint not found: {ck_file}")

        ck_bytes = ck_file.read_bytes()
        ck_s3_key = manifest_prefix + f"{target_checkpoint_version:020}.checkpoint"
        transport.put_object(args.bucket, ck_s3_key, ck_bytes)
        print(f"  PUT {ck_s3_key} ({len(ck_bytes)} bytes)")

        last_ck_file = generated_dir / "_last_checkpoint"
        if not last_ck_file.exists():
            fail(f"Generated _last_checkpoint not found: {last_ck_file}")
        last_ck_bytes = last_ck_file.read_bytes()
        last_ck_s3_key = manifest_prefix + "_last_checkpoint"
        transport.put_object(args.bucket, last_ck_s3_key, last_ck_bytes)
        print(f"  PUT {last_ck_s3_key} ({len(last_ck_bytes)} bytes)")

        write_json(str(out_dir / "put-manifest-summary.json"), {
            "checkpoint_key": ck_s3_key,
            "checkpoint_bytes": len(ck_bytes),
            "last_checkpoint_key": last_ck_s3_key,
            "last_checkpoint_bytes": len(last_ck_bytes),
            "materialized_active_objects": args.materialize_active_objects,
            "materialized_parquet_count": materialized_parquet_count,
        })

        # ---- 12. Scale datanode back to 1 --------------------------------
        print(f"\n[{now_iso()}] scaling datanode StatefulSet back to 1...")
        scale_start = time.perf_counter()
        subprocess.run([
            "kubectl", "scale", "statefulset", sts_name,
            "-n", args.namespace, "--replicas=1",
        ], check=True)
        datanode_scaled_down = False
        print(f"  scaled {sts_name} → 1")

        # Wait for pod Ready
        print(f"  waiting for datanode pod Ready...")
        deadline = time.time() + 300
        pod_ready = False
        while time.time() < deadline:
            pod_name = kubectl_pod_name_optional(label, args.namespace)
            if not pod_name:
                time.sleep(5)
                continue
            pod_json = kubectl_json([
                "get", "pod", "-n", args.namespace, pod_name,
            ])
            conditions = (
                pod_json.get("status", {}).get("conditions", [])
            )
            ready_cond = next(
                (c for c in conditions if c.get("type") == "Ready"), None
            )
            if ready_cond and ready_cond.get("status") == "True":
                pod_ready = True
                break
            else:
                time.sleep(5)
        if not pod_ready:
            fail("datanode pod did not become Ready within 300s")
        scale_elapsed = time.perf_counter() - scale_start
        elapsed["datanode_scale"] = scale_elapsed
        print(f"  datanode Ready in {scale_elapsed:.2f}s")

        # ---- 13. Settle + region stats -----------------------------------
        print(f"\n[{now_iso()}] settling for {args.settle_seconds}s...")
        time.sleep(args.settle_seconds)

        print(f"\n[{now_iso()}] querying region statistics after manifest swap...")
        region_stats_after = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-after-swap.json"), region_stats_after)

        # Extract sst_num
        def _extract_first_row(resp: Dict) -> Optional[Dict[str, Any]]:
            rows = (resp.get("response", {}).get("output", [{}])[0]
                    .get("records", {}).get("rows", []))
            if rows:
                cols = ["region_id", "table_id", "region_number",
                        "manifest_size", "sst_num", "sst_size", "memtable_size"]
                return dict(zip(cols, rows[0]))
            return None

        rs_first = _extract_first_row(region_stats_after)
        sst_num_actual = rs_first.get("sst_num", 0) if rs_first else 0
        sst_num_expected = args.count
        print(f"  sst_num = {sst_num_actual} (expected {sst_num_expected})")
        if rs_first:
            print(f"  manifest_size = {rs_first.get('manifest_size', '?')}")
        if int(sst_num_actual) != int(sst_num_expected):
            print(
                f"\n[{now_iso()}] FAIL: sst_num mismatch "
                f"({sst_num_actual} != {sst_num_expected})",
                file=sys.stderr,
            )
            gate_status = "failed"

        # ---- 14. S3 count after swap -------------------------------------
        print(f"\n[{now_iso()}] S3 count after manifest swap...")
        after_swap = count_s3_objects(transport, args.bucket, region_prefix)
        print(f"  total={after_swap['total']} parquet={after_swap['parquet']} "
              f"manifest={after_swap['manifest']}")

        # ---- 15. Prometheus snapshots ------------------------------------
        prom_queries = {
            "mito_gc_all": '{__name__=~"greptime_mito_gc.*"}',
            "metasrv_gc_all": '{__name__=~"greptime_metasrv_gc.*"}',
            "opendal_s3_delete_count":
                'opendal_http_request_duration_seconds_count{scheme="s3",operation="delete"}',
            "opendal_s3_delete_sum":
                'opendal_http_request_duration_seconds_sum{scheme="s3",operation="delete"}',
            "manifest_ops": '{__name__=~"greptime_manifest.*"}',
            "gc_ops": '{__name__=~"greptime_mito_gc.*"}',
            "memory_working_set": 'container_memory_working_set_bytes{namespace="' + args.namespace + '"}',
        }
        baseline_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "baseline-before-gc")

        # ---- 16. Fast GC probe -------------------------------------------
        fast_gc_elapsed = 0.0
        after_fast_gc = dict(after_swap)
        if args.skip_fast_gc:
            print(f"\n[{now_iso()}] skipping fast GC probe (--skip-fast-gc)")
        else:
            print(f"\n[{now_iso()}] fast GC before snapshot...")
            fast_before_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir), "before-fast-gc")

            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}) (fast)...")
            gc_result, fast_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id})",
                                  args.http_timeout))
            print(f"  fast GC completed in {fast_gc_elapsed:.2f}s")
            elapsed["fast_gc"] = fast_gc_elapsed
            code, text = gc_result
            write_json(str(out_dir / "sql-fast-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id})",
                        "status_code": code, "response_text": text})

            after_fast_gc = count_s3_objects(transport, args.bucket, region_prefix)
            print(f"  s3 after fast GC: total={after_fast_gc['total']} "
                  f"parquet={after_fast_gc['parquet']} manifest={after_fast_gc['manifest']}")

            fast_after_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir), "after-fast-gc")
            fast_delta = compute_metrics_delta(fast_before_metrics, fast_after_metrics)
            write_json(str(out_dir / "metrics-delta-fast-gc.json"), fast_delta)

            if after_fast_gc["parquet"] < after_swap["parquet"]:
                print(f"\n[{now_iso()}] FAIL: parquet decreased after fast GC", file=sys.stderr)
                gate_status = "failed"
            od_count = fast_delta.get("opendal_s3_delete_count", {}).get("changed_or_new", [])
            for c in od_count:
                if c.get("delta", 0) > 0:
                    print(f"\n[{now_iso()}] FAIL: S3 delete metric delta > 0 after fast GC", file=sys.stderr)
                    gate_status = "failed"

        # ---- 17. Full GC probe -------------------------------------------
        full_gc_elapsed = 0.0
        after_full_gc = dict(after_fast_gc)
        if args.skip_full_gc:
            print(f"\n[{now_iso()}] skipping full GC probe (--skip-full-gc)")
        else:
            print(f"\n[{now_iso()}] full GC before snapshot...")
            full_before_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir), "before-full-gc")

            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}, true) (full)...")
            full_result, full_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id}, true)",
                                  args.http_timeout))
            print(f"  full GC completed in {full_gc_elapsed:.2f}s")
            elapsed["full_gc"] = full_gc_elapsed
            code2, text2 = full_result
            write_json(str(out_dir / "sql-full-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id}, true)",
                        "status_code": code2, "response_text": text2})

            after_full_gc = count_s3_objects(transport, args.bucket, region_prefix)
            print(f"  s3 after full GC: total={after_full_gc['total']} "
                  f"parquet={after_full_gc['parquet']} manifest={after_full_gc['manifest']}")

            full_after_metrics = metrics_snapshot_set(
                args.monitor_local_port, prom_queries, str(out_dir), "after-full-gc")
            full_delta = compute_metrics_delta(full_before_metrics, full_after_metrics)
            write_json(str(out_dir / "metrics-delta-full-gc.json"), full_delta)

            comparison_base = after_swap if args.skip_fast_gc else after_fast_gc
            if after_full_gc["parquet"] < comparison_base["parquet"]:
                print(f"\n[{now_iso()}] FAIL: parquet decreased after full GC", file=sys.stderr)
                gate_status = "failed"

            # C2b: if materialized placeholder objects were written, verify
            # they survived full GC (active-object protection).  The
            # expected minimum post-GC parquet count is the pre-existing
            # count plus the materialized placeholder count.
            if args.materialize_active_objects and materialized_parquet_count > 0:
                expected_min = pre_materialize_parquet_count + materialized_parquet_count
                actual = after_full_gc["parquet"]
                if actual < expected_min:
                    print(
                        f"\n[{now_iso()}] FAIL: materialized parquet objects "
                        f"may have been deleted by full GC: "
                        f"expected >= {expected_min}, got {actual} "
                        f"(pre_existing={pre_materialize_parquet_count}, "
                        f"materialized={materialized_parquet_count})",
                        file=sys.stderr,
                    )
                    gate_status = "failed"
                else:
                    print(
                        f"  C2b active-object check PASSED: "
                        f"parquet after full GC ({actual}) >= expected min ({expected_min})"
                    )
            od_count = full_delta.get("opendal_s3_delete_count", {}).get("changed_or_new", [])
            for c in od_count:
                if c.get("delta", 0) > 0:
                    print(f"\n[{now_iso()}] FAIL: S3 delete metric delta > 0 after full GC", file=sys.stderr)
                    gate_status = "failed"

        # ---- 18. Cluster after + logs ------------------------------------
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

        if cluster_phase != "Running":
            print(f"\n[{now_iso()}] FAIL: cluster phase '{cluster_phase}' != Running", file=sys.stderr)
            gate_status = "failed"

        # Check pod restarts
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
                    print(f"\n[{now_iso()}] FAIL: pod {after_name} restart increased", file=sys.stderr)
                    gate_status = "failed"

        # Fetch logs
        print(f"\n[{now_iso()}] fetching logs (since={args.logs_since})...")
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

        # Parse GC reports + log evidence
        gc_reports = parse_gc_reports(meta_logs, region_id)
        write_json(str(out_dir / "gc-report-log-summary.json"),
                   {"meta_gc_report_summaries": gc_reports})
        log_evidence = count_log_evidence(meta_logs, dn_logs)

        # ---- 19. Metrics delta (overall) ---------------------------------
        final_metrics = metrics_snapshot_set(
            args.monitor_local_port, prom_queries, str(out_dir), "final")
        metrics_delta = compute_metrics_delta(baseline_metrics, final_metrics)

        # ---- 20. Region stats summary ------------------------------------
        region_stats_summary = {
            "after_swap": _extract_first_row(region_stats_after),
        }

        # ---- 21. Write outputs -------------------------------------------
        object_counts = {
            "after_swap": after_swap,
            "after_fast_gc": after_fast_gc,
            "after_full_gc": after_full_gc,
        }
        metrics_summary = {
            "count": args.count,
            "table_id": table_id,
            "region_id": region_id,
            "prefix": region_prefix,
            "manifest_prefix": manifest_prefix,
            "checkpoint_version": target_checkpoint_version,
            "object_counts": object_counts,
            "sst_num_expected": sst_num_expected,
            "sst_num_actual": sst_num_actual,
            "region_stats_summary": region_stats_summary,
            "gc_report_log_summary": gc_reports,
            "log_evidence": log_evidence,
            "elapsed": elapsed,
            "metrics_delta": metrics_delta,
            "gate_status": gate_status,
        }
        write_json(str(out_dir / "metrics-summary.json"), metrics_summary)

        concise = build_concise_summary(
            str(out_dir), args, table_id, region_id, region_prefix,
            object_counts, elapsed, metrics_delta, gc_reports,
            pods_after, cluster_phase, log_evidence, region_stats_summary,
            gate_status, sst_num_expected,
        )
        write_text(str(out_dir / "concise-summary.txt"), concise)
        print(f"\n[{now_iso()}] all evidence written to {out_dir}/")
        print(concise)

        if gate_status == "failed":
            print(f"\n[{now_iso()}] gate_status=failed; exiting with code 1")
            sys.exit(1)

    finally:
        if args.execute and datanode_scaled_down:
            print(
                f"\n[{now_iso()}] cleanup: datanode may still be scaled to 0; "
                f"scaling {sts_name} back to 1...",
                file=sys.stderr,
            )
            try:
                subprocess.run([
                    "kubectl", "scale", "statefulset", sts_name,
                    "-n", args.namespace, "--replicas=1",
                ], check=False)
            except Exception as e:
                print(f"cleanup scale-up failed: {e}", file=sys.stderr)
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
