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
run_repartition_gc_correctness_smoke.py
Phase 3 GC + repartition correctness smoke: fresh table -> insert -> flush ->
split partition -> prove cross-region refs -> GC on source -> verify object
survival and SQL readability.

Flow:
1. Create fresh metric physical table and logical table.
2. Insert 5 deterministic namespace/value rows (app-0, app-1, app-2,
   app-10, app-15) + flush. Timestamps use wall clock time.
3. Capture before: partitions, region IDs, S3 objects, table_id.
4. Run SPLIT PARTITION on range app-1..app-2 -> app-1..app-10, app-10..app-2.
5. Poll until partition count changes (3 -> 4) and SQL reads stable.
6. Identify source/destination region IDs from information_schema.partitions.
7. Back up source + destination manifest objects from MinIO.
8. Call Rust gc_region_manifest_summary to prove cross-region refs. The harness
   path replays uncompressed data-manifest delta JSON files from the backup; it
   does not combine checkpoints with later deltas or handle `.json.gz` deltas.
9. Run ADMIN GC_REGIONS(source_region) (fast).
10. Verify referenced source SST objects still exist + SQL reads still correct.
11. Capture evidence: SQL responses, S3 counts, manifest scan, concise-summary.

Dry-run (no --execute): prints plan and exits.

See .slim/deepwork/gc-repartition-correctness-smoke.md for the plan.
"""

from __future__ import annotations

import argparse
import json
import os
import re
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
        description="GC + repartition correctness smoke harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--out", required=True,
                   help="Output directory for evidence files")

    # Cluster
    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--db", default="metrics_gc_repart_smoke")
    g.add_argument("--physical-table", default="greptime_physical_table")
    g.add_argument("--table", default="gc_repart_logical")
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
    g.add_argument("--storage-path", default=None,
                   help="Path component inside root-prefix "
                        "(default: greptime/<db>; override for custom layouts)")

    # Rate
    g = p.add_argument_group("rate")
    g.add_argument("--http-timeout", type=float, default=600.0)
    g.add_argument("--settle-seconds", type=int, default=30)
    g.add_argument("--split-poll-timeout", type=int, default=120,
                   help="Max seconds to wait for split completion")

    # Flags
    g = p.add_argument_group("flags")
    g.add_argument("--execute", action="store_true",
                   help="Execute the full flow. Without this, dry-run only.")
    g.add_argument("--create-table", action="store_true",
                   help="Create table if absent; fail if absent and flag not provided")
    g.add_argument("--skip-fast-gc", action="store_true")
    g.add_argument("--forbid-table-id", type=int, action="append", default=None,
                   help="Forbid a table_id (repeatable; default includes 1035)")
    g.add_argument("--generator-bin", default=None,
                   help="Path or command for Rust scanner "
                        "(default: cargo run -p cmd --bin gc_region_manifest_summary --)")
    g.add_argument("--fail-if-no-cross-refs", dest="fail_if_no_cross_refs",
                   action="store_true", default=True,
                   help="Fail if destination manifest has no cross-region refs (default: true)")
    g.add_argument("--allow-no-cross-refs", dest="fail_if_no_cross_refs",
                   action="store_false",
                   help="Warn instead of failing when destination manifest has no cross-region refs")

    return p


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_json(path: str, obj: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def write_text(path: str, text: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


def partition_description_has_value(description: str, value: str) -> bool:
    """Match a partition literal without treating app-1 as a match for app-10."""
    pattern = rf"(?<![A-Za-z0-9_-]){re.escape(value)}(?![A-Za-z0-9_-])"
    return re.search(pattern, description) is not None


def partition_matches_range(partition: Dict[str, Any], lower: str, upper: str) -> bool:
    description = str(partition.get("partition_description", ""))
    return (
        partition_description_has_value(description, lower)
        and partition_description_has_value(description, upper)
    )


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


# ---- HTTP: SQL ------------------------------------------------------------
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


def sql_query_ok(port: int, sql: str, timeout: float = 300.0) -> Any:
    code, text = sql_query(port, sql, timeout)
    if code != 200:
        raise RuntimeError(f"SQL failed HTTP {code}: {text[:500]}")
    return json.loads(text)


def sql_query_ok_rows(port: int, sql: str, timeout: float = 300.0) -> List:
    resp = sql_query_ok(port, sql, timeout)
    return resp.get("output", [{}])[0].get("records", {}).get("rows", [])


# ---- kubectl helpers ------------------------------------------------------
def kubectl(args: List[str]) -> str:
    return subprocess.check_output(["kubectl"] + args, text=True)


def kubectl_json(args: List[str]) -> Any:
    return json.loads(kubectl(args + ["-o", "json"]))


def kubectl_pod_name_optional(label: str, namespace: str) -> Optional[str]:
    pod_list = kubectl_json(["get", "pod", "-n", namespace, "-l", label])
    items = pod_list.get("items", [])
    if not items:
        return None
    return items[0].get("metadata", {}).get("name", "") or None


# ---- S3 helpers -----------------------------------------------------------
def s3_get_object(transport: S3Transport, bucket: str, key: str) -> bytes:
    try:
        _, data = transport._request("GET", bucket, key)
        return data
    except RuntimeError as e:
        fail(f"S3 GET {bucket}/{key} failed: {e}")
        raise


def s3_head_object(transport: S3Transport, bucket: str, key: str) -> bool:
    try:
        transport._request("HEAD", bucket, key)
        return True
    except RuntimeError as e:
        msg = str(e)
        if "HTTP 404" in msg or "NoSuchKey" in msg:
            return False
        raise


def count_s3_objects(transport: S3Transport, bucket: str,
                     prefix: str) -> Dict[str, int]:
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
    result = []
    for key, size in transport.list_objects_v2(bucket, prefix):
        result.append({"key": key, "size": size})
    return result


def max_manifest_object_version(objects: List[Dict[str, Any]]) -> int:
    max_version = 0
    for obj in objects:
        name = os.path.basename(obj["key"])
        m = re.match(r"^(\d{20})\.(?:checkpoint|json)(?:\.gz)?$", name)
        if m:
            max_version = max(max_version, int(m.group(1)))
    return max_version


# ---- Region prefix helpers ------------------------------------------------
def region_id_components(region_id: int) -> Tuple[int, int]:
    table_id = region_id >> 32
    # Object-store region directory uses region_sequence, not region_number with
    # metric-engine group bits. For normal mito regions these are the same.
    region_sequence = region_id & 0x00FFFFFF
    return table_id, region_sequence


def region_prefix_for(root_prefix: str, storage_path: str, region_id: int) -> str:
    table_id, region_number = region_id_components(region_id)
    region_dir = f"{table_id}_{region_number:010d}"
    return f"{root_prefix}/data/{storage_path}/{table_id}/{region_dir}/"


# ---- Manifest backup (handles both bare and data path layouts) ------------
def backup_region_manifests(
    transport: S3Transport, bucket: str,
    region_prefix: str, region_id: int, out_dir: Path, label: str
) -> Tuple[Path, List[Dict[str, Any]], str]:
    """Back up manifest objects for a region. Tries bare path then data path."""
    candidates = [
        region_prefix + "manifest/",
        region_prefix + "data/manifest/",
    ]
    manifest_objects: List[Dict[str, Any]] = []
    used_prefix: str = ""

    for pfx in candidates:
        objects = list_s3_objects(transport, bucket, pfx)
        if objects:
            manifest_objects = objects
            used_prefix = pfx
            print(f"  {label}: found {len(objects)} manifest objects at {pfx}")
            break

    if not manifest_objects:
        fail(f"{label}: no manifest objects found at any candidate prefix for region {region_id}")

    backup_dir = out_dir / f"manifest-backup-{label}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    for obj in manifest_objects:
        relative_key = obj["key"]
        if relative_key.startswith(used_prefix):
            rel_path = relative_key[len(used_prefix):]
        else:
            rel_path = os.path.basename(relative_key)
        local_path = backup_dir / rel_path
        data = s3_get_object(transport, bucket, relative_key)
        local_path.write_bytes(data)

    write_json(str(out_dir / f"manifest-objects-{label}.json"), manifest_objects)
    return backup_dir, manifest_objects, used_prefix


# ---- Information schema queries -------------------------------------------
def query_table_id(port: int, db: str, table: str, timeout: float) -> int:
    rows = sql_query_ok_rows(port, (
        f"SELECT table_id FROM information_schema.tables "
        f"WHERE table_name='{table}' AND table_schema='{db}'"
    ), timeout)
    if not rows:
        fail(f"No table_id found for {db}.{table}")
    return int(rows[0][0])


def query_partitions(port: int, db: str, table: str, timeout: float) -> List[Dict[str, Any]]:
    """Return partition rows for a table."""
    rows = sql_query_ok_rows(port, (
        f"SELECT partition_name, partition_description, greptime_partition_id "
        f"FROM information_schema.partitions "
        f"WHERE table_name='{table}' AND table_schema='{db}' "
        f"ORDER BY greptime_partition_id"
    ), timeout)
    return [
        {"partition_name": r[0], "partition_description": r[1], "region_id": int(r[2])}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Dry-run plan
# ---------------------------------------------------------------------------
def dry_run_plan(args: argparse.Namespace) -> None:
    print("=== DRY-RUN PLAN ===")
    print(f"Mode:          dry-run (no cluster/object-store writes)")
    print(f"DB:            {args.db}")
    print(f"Physical table:{args.physical_table}")
    print(f"Logical table: {args.table}")
    print(f"Storage path:  {args.storage_path}")
    print(f"Namespace:     {args.namespace}")
    print(f"Out dir:       {args.out}")
    print(f"Execute:       {args.execute}")
    print(f"Create table:  {args.create_table}")
    print()
    print("Planned steps:")
    print("  1. port-forward frontend/monitor/minio")
    print("  2. create fresh metric table (if --create-table)")
    print("  3. insert 5 deterministic namespace/value rows + flush")
    print("  4. capture before partitions/region IDs/S3 objects")
    print("  5. run SPLIT PARTITION (app-1..app-2 -> app-1..app-10, app-10..app-2)")
    print("  6. poll for split completion (partitions 3->4, SQL stable)")
    print("  7. identify source/destination region IDs")
    print("  8. backup source + destination manifests from S3")
    print("  9. run Rust manifest scanner to prove cross-region refs")
    print(" 10. run ADMIN GC_REGIONS(source_region)")
    print(" 11. verify referenced SST existence + SQL reads")
    print(" 12. evidence + concise-summary")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = build_parser().parse_args()

    if args.forbid_table_id is None:
        args.forbid_table_id = [1035]
    if args.storage_path is None:
        args.storage_path = f"greptime/{args.db}"

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

        transport = S3Transport(
            endpoint=args.s3_endpoint,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
        )

        # ---- 2. Create table ---------------------------------------------
        print(f"\n[{now_iso()}] setting up tables in {args.db}...")
        table_check_sql = (
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema='{args.db}' "
            f"AND table_name IN ('{args.physical_table}', '{args.table}')"
        )
        code, text = sql_query(args.frontend_local_port, table_check_sql, args.http_timeout)
        if code != 200:
            fail(f"table existence query failed HTTP {code}: {text[:500]}")
        resp = json.loads(text)
        tables = resp.get("output", [{}])[0].get("records", {}).get("rows", [])
        existing_tables = [r[0] for r in tables]

        if existing_tables:
            fail(
                f"Tables already exist in {args.db}: {existing_tables}. "
                f"This harness requires a fresh table. Drop it first."
            )
        if not args.create_table:
            fail(f"Table {args.db}.{args.table} not found and --create-table not set")

        # Create database
        code, text = sql_query(args.frontend_local_port,
                               f"CREATE DATABASE IF NOT EXISTS {args.db}",
                               args.http_timeout)
        if code != 200:
            fail(f"CREATE DATABASE failed HTTP {code}: {text[:200]}")
        print(f"  database {args.db} ready")

        # Create metric physical table using the V2/e2e shape.
        physical_ddl = (
            f"CREATE TABLE {args.db}.{args.physical_table} ("
            f"greptime_timestamp TIMESTAMP(3) NOT NULL, "
            f"greptime_value DOUBLE NULL, "
            f"`namespace` STRING NULL INVERTED INDEX, "
            f"PRIMARY KEY (`namespace`), "
            f"TIME INDEX (greptime_timestamp)"
            f") "
            f"PARTITION ON COLUMNS (`namespace`) ("
            f"`namespace` < 'app-1', "
            f"`namespace` >= 'app-1' AND `namespace` < 'app-2', "
            f"`namespace` >= 'app-2'"
            f") "
            f"ENGINE = metric WITH ("
            f"'physical_metric_table' = '', "
            f"'memtable.type' = 'partition_tree', "
            f"'sst_format' = 'flat', "
            f"'memtable.partition_tree.primary_key_encoding' = 'sparse', "
            f"'index.type' = 'inverted'"
            f")"
        )
        code, text = sql_query(args.frontend_local_port, physical_ddl, args.http_timeout)
        if code != 200:
            fail(f"CREATE PHYSICAL TABLE failed HTTP {code}: {text[:500]}")
        print(f"  physical table {args.db}.{args.physical_table} created (3 partitions)")

        # Create logical metric table for writes/reads, mapped to the physical table.
        logical_ddl = (
            f"CREATE TABLE {args.db}.{args.table} ("
            f"greptime_timestamp TIMESTAMP(3) NOT NULL, "
            f"greptime_value DOUBLE NULL, "
            f"`namespace` STRING NULL, "
            f"PRIMARY KEY (`namespace`), "
            f"TIME INDEX (greptime_timestamp)"
            f") ENGINE = metric WITH ("
            f"'on_physical_table' = '{args.physical_table}'"
            f")"
        )
        code, text = sql_query(args.frontend_local_port, logical_ddl, args.http_timeout)
        if code != 200:
            fail(f"CREATE LOGICAL TABLE failed HTTP {code}: {text[:500]}")
        print(f"  logical table {args.db}.{args.table} created on {args.physical_table}")

        # Find physical table_id; region prefixes are physical data-region prefixes.
        table_id = query_table_id(args.frontend_local_port, args.db, args.physical_table, args.http_timeout)
        logical_table_id = query_table_id(args.frontend_local_port, args.db, args.table, args.http_timeout)
        print(f"  physical table_id = {table_id}")
        print(f"  logical table_id = {logical_table_id}")

        if table_id in (args.forbid_table_id or []):
            fail(f"table_id={table_id} is in forbidden list {args.forbid_table_id}")

        # ---- 3. Insert rows + flush --------------------------------------
        print(f"\n[{now_iso()}] inserting 5 deterministic namespace/value rows...")
        rows_data = [
            ("app-0", 0.0),
            ("app-1", 1.0),
            ("app-2", 2.0),
            ("app-10", 10.0),
            ("app-15", 15.0),
        ]
        for ns, val in rows_data:
            ts = int(time.time() * 1000)
            code, text = sql_query(args.frontend_local_port, (
                f"INSERT INTO {args.db}.{args.table} "
                f"(namespace, greptime_timestamp, greptime_value) "
                f"VALUES ('{ns}', {ts}, {val})"
            ), args.http_timeout)
            if code != 200:
                fail(f"INSERT {ns} failed HTTP {code}: {text[:500]}")
        print(f"  inserted {len(rows_data)} rows")

        # Flush
        code, text = sql_query(args.frontend_local_port,
                               f"ADMIN FLUSH_TABLE('{args.db}.{args.physical_table}')",
                               args.http_timeout)
        if code != 200:
            fail(f"FLUSH physical table failed HTTP {code}: {text[:500]}")
        print(f"  flushed")

        # ---- 4. Capture BEFORE state -------------------------------------
        print(f"\n[{now_iso()}] capturing BEFORE state...")
        before_partitions = query_partitions(args.frontend_local_port, args.db,
                                              args.physical_table, args.http_timeout)
        write_json(str(out_dir / "partitions-before.json"), before_partitions)
        print(f"  partitions before: {len(before_partitions)} regions")
        for p in before_partitions:
            print(f"    {p}")

        before_count_rows = sql_query_ok_rows(args.frontend_local_port,
                                                f"SELECT COUNT(*) FROM {args.db}.{args.table}",
                                                args.http_timeout)
        before_count = int(before_count_rows[0][0]) if before_count_rows else -1
        write_json(str(out_dir / "count-before.json"), {"count": before_count})
        print(f"  COUNT(*) = {before_count}")
        if before_count != len(rows_data):
            fail(f"COUNT(*) before split {before_count} != inserted rows {len(rows_data)}")

        # Identify source partition: range app-1..app-2
        source_partitions = [p for p in before_partitions
                             if partition_matches_range(p, "app-1", "app-2")]
        if len(source_partitions) != 1:
            fail(
                f"Expected exactly 1 source partition containing app-1..app-2, "
                f"got {len(source_partitions)}: {source_partitions}"
            )
        source_region_id = source_partitions[0]["region_id"]
        print(f"  source region (app-1..app-2) = {source_region_id}")

        # S3 baseline
        source_prefix = region_prefix_for(args.root_prefix, args.storage_path,
                                          source_region_id)
        s3_before = count_s3_objects(transport, args.bucket, source_prefix)
        write_json(str(out_dir / "s3-counts-source-before.json"), s3_before)
        print(f"  S3 before split: source region {source_region_id}: {s3_before}")

        # ---- 5. Run SPLIT PARTITION --------------------------------------
        split_start = time.perf_counter()
        print(f"\n[{now_iso()}] running SPLIT PARTITION on app-1..app-2...")
        split_sql = (
            f"ALTER TABLE {args.db}.{args.physical_table} "
            f"SPLIT PARTITION (`namespace` >= 'app-1' AND `namespace` < 'app-2') "
            f"INTO ("
            f"`namespace` >= 'app-1' AND `namespace` < 'app-10', "
            f"`namespace` >= 'app-10' AND `namespace` < 'app-2'"
            f")"
        )
        code, text = sql_query(args.frontend_local_port, split_sql, args.http_timeout)
        write_json(str(out_dir / "sql-split-response.json"),
                   {"sql": split_sql, "status_code": code, "response": text})
        print(f"  SPLIT PARTITION returned HTTP {code}")
        if code != 200:
            fail(f"SPLIT PARTITION failed HTTP {code}: {text[:500]}")

        # ---- 6. Poll for split completion ---------------------------------
        print(f"\n[{now_iso()}] polling for split completion...")
        deadline = time.time() + args.split_poll_timeout
        after_partitions: List[Dict[str, Any]] = []
        while time.time() < deadline:
            after_partitions = query_partitions(args.frontend_local_port, args.db,
                                                 args.physical_table, args.http_timeout)
            if len(after_partitions) == 4:
                # Verify SQL reads stable for app-1 and app-10 ranges
                count_rows = sql_query_ok_rows(args.frontend_local_port, (
                    f"SELECT COUNT(*) FROM {args.db}.{args.table}"
                ), args.http_timeout)
                current_count = int(count_rows[0][0]) if count_rows else -1
                if current_count == before_count:
                    print(f"  split complete: {len(after_partitions)} partitions, COUNT(*) = {current_count}")
                    break
            time.sleep(5)
        else:
            fail(f"Split did not complete within {args.split_poll_timeout}s. "
                 f"Partitions: {len(after_partitions)}")

        write_json(str(out_dir / "partitions-after.json"), after_partitions)
        print(f"  partitions after: {len(after_partitions)} regions")
        for p in after_partitions:
            print(f"    {p}")

        split_elapsed = time.perf_counter() - split_start
        elapsed["split"] = split_elapsed
        print(f"  split completed in {split_elapsed:.2f}s")

        # ---- 7. Identify split child / destination regions -----------------
        print(f"\n[{now_iso()}] identifying split child regions...")
        # GreptimeDB can reuse the source region for one split half and create
        # only one new destination region. Identify the two child partitions by
        # range expression first, then scan only children whose region_id differs
        # from the source region for cross-region refs.
        left_children = [p for p in after_partitions
                         if partition_matches_range(p, "app-1", "app-10")]
        right_children = [p for p in after_partitions
                          if partition_matches_range(p, "app-10", "app-2")]
        if len(left_children) != 1 or len(right_children) != 1:
            fail(
                f"Expected exactly one split child for each range, got "
                f"left={left_children}, right={right_children}"
            )

        split_child_partitions = [left_children[0], right_children[0]]
        split_child_region_ids = [p["region_id"] for p in split_child_partitions]
        dest_partitions = [
            p for p in split_child_partitions
            if p["region_id"] != source_region_id
        ]
        if not dest_partitions:
            fail(
                f"Split children did not create any non-source destination region: "
                f"source_region_id={source_region_id}, children={split_child_partitions}"
            )

        dest_region_ids = sorted({p["region_id"] for p in dest_partitions})
        write_json(str(out_dir / "split-child-partitions.json"), {
            "source_region_id": source_region_id,
            "split_child_partitions": split_child_partitions,
            "split_child_region_ids": split_child_region_ids,
            "destination_partitions": dest_partitions,
            "destination_region_ids": dest_region_ids,
        })
        print(f"  split child regions: {split_child_region_ids}")
        print(f"  non-source destination regions: {dest_region_ids}")

        # ---- 8. Backup source + destination manifests ---------------------
        print(f"\n[{now_iso()}] backing up source + destination manifests...")
        # Source manifest backup
        src_backup_dir, src_objects, src_manifest_prefix = backup_region_manifests(
            transport, args.bucket, source_prefix, source_region_id, out_dir, "source")
        source_data_prefix = src_manifest_prefix.removesuffix("manifest/")
        # Destination manifest backups
        dest_backups: List[Tuple[int, Path]] = []
        for drid in dest_region_ids:
            dprefix = region_prefix_for(args.root_prefix, args.storage_path, drid)
            dbackup_dir, _, _ = backup_region_manifests(
                transport, args.bucket, dprefix, drid, out_dir, f"dest-{drid}")
            dest_backups.append((drid, dbackup_dir))

        # ---- 9. Run Rust manifest scanner on destination manifests -------
        print(f"\n[{now_iso()}] running Rust manifest scanner on destination manifests...")
        if args.generator_bin:
            gen_cmd_prefix = args.generator_bin.split()
        else:
            gen_cmd_prefix = [
                "cargo", "run", "-p", "cmd",
                "--bin", "gc_region_manifest_summary", "--",
            ]

        cross_ref_found = False
        cross_ref_details: Dict[str, Any] = {}
        referenced_source_file_ids = set()
        for drid, dbackup_dir in dest_backups:
            summary_path = out_dir / f"manifest-summary-dest-{drid}.json"
            scanner_args = gen_cmd_prefix + [
                "--region-id", str(drid),
                "--seed-delta-dir", str(dbackup_dir),
                "--output", str(summary_path),
            ]
            scan_result = subprocess.run(
                scanner_args, capture_output=True, text=True,
                cwd=os.path.dirname(_SCRIPT_DIR),
            )
            if scan_result.returncode != 0:
                fail(f"Scanner failed for dest region {drid}: {scan_result.stderr}")

            summary = json.loads(summary_path.read_text())

            cross_count = summary.get("cross_region_file_count", 0)
            print(f"  dest region {drid}: {summary['file_count']} files, "
                  f"{cross_count} cross-region, "
                  f"file_region_id_counts={summary.get('file_region_id_counts', {})}")

            # Check for source region_id as origin
            src_ref_files = [
                f for f in summary.get("files", [])
                if f.get("file_region_id") == source_region_id
                and f.get("current_region_id") == drid
            ]
            if src_ref_files:
                cross_ref_found = True
                referenced_source_file_ids.update(f["file_id"] for f in src_ref_files)
            cross_ref_details[str(drid)] = {
                "cross_region_count": cross_count,
                "source_ref_file_count": len(src_ref_files),
                "source_ref_file_ids": [f["file_id"] for f in src_ref_files],
            }

        write_json(str(out_dir / "cross-ref-summary.json"), {
            "source_region_id": source_region_id,
            "split_child_region_ids": split_child_region_ids,
            "dest_region_ids": dest_region_ids,
            "cross_ref_found": cross_ref_found,
            "referenced_source_file_ids": sorted(referenced_source_file_ids),
            "details": cross_ref_details,
        })

        if not cross_ref_found and args.fail_if_no_cross_refs:
            fail(
                f"No cross-region refs found in destination manifests! "
                f"Check split/remap. See manifest-summary-dest-*.json for details."
            )
        elif not cross_ref_found:
            print(
                f"  WARNING: no cross-region refs found (--allow-no-cross-refs set)",
                file=sys.stderr,
            )

        # ---- 10. Run ADMIN GC_REGIONS on source --------------------------
        if args.skip_fast_gc:
            print(f"\n[{now_iso()}] skipping ADMIN GC_REGIONS({source_region_id}) by flag")
            elapsed["gc_source"] = 0.0
            write_json(str(out_dir / "sql-gc-source.json"),
                       {"sql": f"ADMIN GC_REGIONS({source_region_id})",
                        "status_code": "skipped", "response_text": "skipped by --skip-fast-gc"})
        else:
            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({source_region_id}) (fast)...")
            gc_start = time.perf_counter()
            code, text = sql_query(args.frontend_local_port,
                                   f"ADMIN GC_REGIONS({source_region_id})",
                                   args.http_timeout)
            gc_elapsed = time.perf_counter() - gc_start
            elapsed["gc_source"] = gc_elapsed
            write_json(str(out_dir / "sql-gc-source.json"),
                       {"sql": f"ADMIN GC_REGIONS({source_region_id})",
                        "status_code": code, "response_text": text})
            if code != 200:
                print(f"  WARNING: GC returned HTTP {code}: {text[:500]}", file=sys.stderr)
                gate_status = "gc_failed"
            else:
                print(f"  GC on source region completed in {gc_elapsed:.2f}s")

        # ---- 11. S3 existence + SQL verification -------------------------
        print(f"\n[{now_iso()}] verifying referenced SST existence after GC...")
        s3_after = count_s3_objects(transport, args.bucket, source_prefix)
        write_json(str(out_dir / "s3-counts-source-after-gc.json"), s3_after)
        print(f"  S3 after GC: {s3_after}")

        # Check that parquet count hasn't decreased below pre-split count
        if s3_after["parquet"] < s3_before["parquet"]:
            print(
                f"  WARNING: parquet count decreased: "
                f"{s3_before['parquet']} -> {s3_after['parquet']} "
                f"(source region may have been fully GCed if refs were released)",
                file=sys.stderr,
            )

        referenced_survival = []
        missing_referenced = []
        for file_id in sorted(referenced_source_file_ids):
            key = f"{source_data_prefix}{file_id}.parquet"
            present = s3_head_object(transport, args.bucket, key)
            referenced_survival.append({"file_id": file_id, "key": key, "present_after_gc": present})
            if not present:
                missing_referenced.append(file_id)
        write_json(str(out_dir / "referenced-source-file-survival.json"), referenced_survival)
        print(
            f"  referenced source files present after GC: "
            f"{len(referenced_source_file_ids) - len(missing_referenced)}/{len(referenced_source_file_ids)}"
        )
        if missing_referenced:
            fail(f"Referenced source files missing after GC: {missing_referenced}")

        # SQL reads still correct
        after_count_rows = sql_query_ok_rows(args.frontend_local_port,
                                              f"SELECT COUNT(*) FROM {args.db}.{args.table}",
                                              args.http_timeout)
        after_count = int(after_count_rows[0][0]) if after_count_rows else -1
        write_json(str(out_dir / "count-after-gc.json"), {"count": after_count})
        print(f"  COUNT(*) after GC = {after_count}")

        if after_count != before_count:
            fail(f"COUNT(*) after GC {after_count} != before split {before_count}")

        # Targeted reads
        for test_ns in ["app-1", "app-10", "app-15"]:
            rows = sql_query_ok_rows(args.frontend_local_port, (
                f"SELECT namespace, greptime_value FROM {args.db}.{args.table} "
                f"WHERE `namespace` = '{test_ns}'"
            ), args.http_timeout)
            print(f"  namespace={test_ns}: {len(rows)} row(s)")
            write_json(str(out_dir / f"sql-read-{test_ns}.json"), rows)
            if len(rows) != 1:
                fail(f"Expected 1 row for namespace={test_ns}, got {len(rows)}")

        # ---- 12. Evidence summary ----------------------------------------
        print(f"\n[{now_iso()}] capturing evidence...")
        pods_after = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-after.json"), pods_after)
        cluster_after = kubectl_json([
            "get", "greptimedbcluster", args.cluster_name, "-n", args.namespace
        ])
        cluster_phase = cluster_after.get("status", {}).get("clusterPhase", "Unknown")
        write_json(str(out_dir / "cluster-after.json"), cluster_after)

        if cluster_phase != "Running":
            print(f"\n[{now_iso()}] FAIL: cluster phase '{cluster_phase}' != Running",
                  file=sys.stderr)
            gate_status = "failed"

        concise_lines = [
            f"EVIDENCE_DIR {args.out}",
            f"PHYSICAL_TABLE {args.db}.{args.physical_table}",
            f"LOGICAL_TABLE {args.db}.{args.table}",
            f"PHYSICAL_TABLE_ID {table_id}",
            f"LOGICAL_TABLE_ID {logical_table_id}",
            f"SOURCE_REGION_ID {source_region_id}",
            f"SPLIT_CHILD_REGION_IDS {split_child_region_ids}",
            f"DEST_REGION_IDS {dest_region_ids}",
            f"PARTITIONS_BEFORE {len(before_partitions)}",
            f"PARTITIONS_AFTER {len(after_partitions)}",
            f"COUNT_BEFORE {before_count}",
            f"COUNT_AFTER {after_count}",
            f"SST_BEFORE_PARQUET {s3_before['parquet']}",
            f"SST_AFTER_PARQUET {s3_after['parquet']}",
            f"CROSS_REF_FOUND {cross_ref_found}",
            f"REFERENCED_SOURCE_FILES {len(referenced_source_file_ids)}",
            f"REFERENCED_SOURCE_FILES_MISSING {len(missing_referenced)}",
            f"GATE_STATUS {gate_status}",
            f"CLUSTER_PHASE {cluster_phase}",
        ]
        for key, val in elapsed.items():
            concise_lines.append(f"{key.upper()}_ELAPSED_SECONDS {val}")

        concise = "\n".join(concise_lines) + "\n"
        write_text(str(out_dir / "concise-summary.txt"), concise)
        print(f"\n[{now_iso()}] evidence written to {out_dir}/")
        print(concise)

        if gate_status != "passed":
            print(f"\n[{now_iso()}] gate_status={gate_status}; exiting with code 1")
            sys.exit(1)

    finally:
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
