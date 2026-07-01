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
run_readable_sst_gc_smoke.py
Phase 3 smoke: fresh table → seed real manifest metadata → generate readable
SST parquet files → offline checkpoint swap → SQL readback → fast/full GC.

Flow:
1. Create fresh table with compatible schema (or verify existing table).
2. Insert one seed row + flush to produce manifest deltas.
3. Query table_id / region_id; compute MinIO key prefixes.
4. Backup manifest objects from S3 (deltas); fail if .json.gz present.
5. Run Rust readable SST fixture generator (gc_readable_sst_fixture) with
   seed deltas; produces real .parquet files + checkpoint manifest.
6. Scale datanode StatefulSet to 0; wait offline.
7. Upload generated .parquet files from object-store tree to MinIO.
8. PUT generated checkpoint then _last_checkpoint to manifest prefix.
9. Scale datanode back to 1; wait Ready.
10. SQL SELECT COUNT(*) must equal sst_count * rows_per_sst.
11. Run fast GC then full GC on region; both should return HTTP 200.
12. Verify generated parquet files survive GC (HEAD each generated object).
13. Capture evidence: SQL responses, S3 counts, summary, logs, concise-summary.

Dry-run (no --execute): prints plan and exits.

See docs/how-to/how-to-test-gc-huge-file-regions.md for the runbook.
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
        description="Phase 3 readable SST GC smoke harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Required / main
    p.add_argument("--sst-count", type=int, default=1,
                   help="Number of SST files to generate (default: 1)")
    p.add_argument("--rows-per-sst", type=int, default=10,
                   help="Number of rows per SST file (default: 10)")
    p.add_argument("--out", required=True,
                   help="Output directory for evidence files")

    # Cluster
    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--table", default="gc_hf_readable_sst_smoke")
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
    g.add_argument("--reuse-existing", action="store_true",
                   help="Allow reusing an existing table (skip table-exists guard)")
    g.add_argument("--allow-large", action="store_true",
                   help="Safety override for large counts")
    g.add_argument("--skip-fast-gc", action="store_true")
    g.add_argument("--skip-full-gc", action="store_true")
    g.add_argument("--skip-readback", action="store_true",
                   help="Pass --skip-readback to fixture generator")
    g.add_argument("--forbid-table-id", type=int, action="append", default=None,
                   help="Forbid a table_id (repeatable; default includes 1035)")
    g.add_argument("--checkpoint-version", type=int, default=1_000_000,
                   help="Minimum checkpoint version to use (default: 1000000; "
                        "the harness also raises this to cover existing manifest versions)")
    g.add_argument("--generator-bin", default=None,
                   help="Path or command for Rust generator "
                        "(default: cargo run -p cmd --bin gc_readable_sst_fixture --)")
    g.add_argument("--row-group-size", type=int, default=50,
                   help="Row group size for parquet writer (default: 50)")

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


def s3_get_object_optional(transport: S3Transport, bucket: str, key: str
                           ) -> Optional[bytes]:
    try:
        _, data = transport._request("GET", bucket, key)
        return data
    except RuntimeError as e:
        msg = str(e)
        if "HTTP 404" in msg or "NoSuchKey" in msg:
            return None
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
    """Return max manifest file version found in checkpoint/delta object names."""
    max_version = 0
    for obj in objects:
        name = os.path.basename(obj["key"])
        m = re.match(r"^(\d{20})\.(?:checkpoint|json)(?:\.gz)?$", name)
        if m:
            max_version = max(max_version, int(m.group(1)))
    return max_version


def s3_head_object(transport: S3Transport, bucket: str, key: str) -> bool:
    """Return True if object exists, False if 404, raise on other errors."""
    try:
        transport._request("HEAD", bucket, key)
        return True
    except RuntimeError as e:
        msg = str(e)
        if "HTTP 404" in msg or "NoSuchKey" in msg:
            return False
        raise


# ---- Region prefix helpers ------------------------------------------------
def region_id_components(region_id: int) -> Tuple[int, int]:
    """Extract (table_id, region_number) from a RegionId u64."""
    table_id = region_id >> 32
    region_number = region_id & 0xFFFFFFFF
    return table_id, region_number


def region_prefix_for(root_prefix: str, storage_path: str, region_id: int) -> str:
    """Build the S3 region prefix for a given region_id."""
    table_id, region_number = region_id_components(region_id)
    region_dir = f"{table_id}_{region_number:010d}"
    return f"{root_prefix}/data/{storage_path}/{table_id}/{region_dir}/"


def table_dir_for(root_prefix: str, storage_path: str, table_id: int) -> str:
    """Build the table_dir string for gc_readable_sst_fixture."""
    return f"{root_prefix}/data/{storage_path}/{table_id}/"


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


# ---------------------------------------------------------------------------
# Dry-run plan
# ---------------------------------------------------------------------------
def dry_run_plan(args: argparse.Namespace) -> None:
    print("=== DRY-RUN PLAN ===")
    print(f"Mode:          dry-run (no cluster/object-store writes)")
    print(f"Table:         {args.table}")
    print(f"Namespace:     {args.namespace}")
    print(f"SST count:     {args.sst_count}")
    print(f"Rows per SST:  {args.rows_per_sst}")
    print(f"Out dir:       {args.out}")
    print(f"Execute:       {args.execute}")
    print(f"Create table:  {args.create_table}")
    print()
    print("Planned steps:")
    print("  1. port-forward frontend/monitor/minio")
    print("  2. create fresh table (if --create-table) or verify absent")
    print("  3. insert seed row + flush")
    print("  4. query table_id / region_id from information_schema")
    print("  5. backup manifest deltas from S3")
    print("  6. run Rust gc_readable_sst_fixture with seed deltas")
    print("  7. scale datanode -> 0")
    print("  8. upload generated .parquet files to MinIO")
    print("  9. PUT generated checkpoint + _last_checkpoint")
    print(" 10. scale datanode -> 1, wait Ready")
    print(" 11. SQL SELECT COUNT(*) validation")
    print(" 12. fast GC + full GC on region")
    print(" 13. verify generated SSTs survive GC")
    print(" 14. evidence + concise-summary")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = build_parser().parse_args()

    if args.forbid_table_id is None:
        args.forbid_table_id = [1035]

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Safety cap
    if args.sst_count > 1000 and not args.allow_large:
        fail(
            f"SST count ({args.sst_count}) exceeds 1000. Use --allow-large to override."
        )

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
        transport = S3Transport(
            endpoint=args.s3_endpoint,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
        )

        # ---- 2. Create / verify table ------------------------------------
        print(f"\n[{now_iso()}] verifying table '{args.table}'...")
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT table_name FROM information_schema.tables "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        resp = json.loads(text)
        tables = (resp.get("output", [{}])[0]
                  .get("records", {}).get("rows", []))
        table_exists = len(tables) > 0

        if table_exists:
            if not args.reuse_existing:
                fail(
                    f"Table '{args.table}' already exists. "
                    f"Use --reuse-existing to allow or drop the table first."
                )
            print(f"  table '{args.table}' exists; reusing (--reuse-existing)")
        else:
            if not args.create_table:
                fail(f"Table '{args.table}' not found and --create-table not set")
            ddl = (
                f"CREATE TABLE {args.table} ("
                f"tag_0 STRING, tag_1 STRING, field_0 UINT64, "
                f"ts TIMESTAMP TIME INDEX, "
                f"PRIMARY KEY(tag_0, tag_1)) "
                f"ENGINE=mito WITH ("
                f"'append_mode' = 'true'"
                f")"
            )
            print(f"  table absent; creating with compatible schema...")
            c_code, c_text = sql_query(args.frontend_local_port, ddl, args.http_timeout)
            if c_code != 200:
                fail(f"CREATE TABLE failed HTTP {c_code}: {c_text[:200]}")
            print(f"  table created.")

        # ---- 3. Insert seed row + flush ----------------------------------
        print(f"\n[{now_iso()}] inserting seed row + flush...")
        seed_value = int(time.time() * 1000) % 1000000000
        seed_sql = (
            f"INSERT INTO {args.table} VALUES "
            f"('seed_tag0', 'seed_tag1', {seed_value}, now())"
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
        print(f"  seed row inserted + flushed")

        # ---- 4. Query table_id / region_id -------------------------------
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

        if table_id in (args.forbid_table_id or []):
            fail(f"table_id={table_id} is in forbidden list {args.forbid_table_id}")

        # Get region_id via greptime_partition_id (single region)
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT greptime_partition_id FROM information_schema.partitions "
                               f"WHERE table_name='{args.table}'",
                               args.http_timeout)
        sql_part = json.loads(text)
        part_rows = sql_part.get("output", [{}])[0].get("records", {}).get("rows", [])
        if len(part_rows) != 1:
            fail(
                f"Expected exactly 1 partition for table '{args.table}', "
                f"got {len(part_rows)}. This tool only supports single-region tables."
            )
        region_id = int(part_rows[0][0])
        print(f"  region_id = {region_id}")

        # Compute prefixes
        region_prefix = region_prefix_for(args.root_prefix, args.storage_path, region_id)
        manifest_prefix = region_prefix + "manifest/"
        table_dir = table_dir_for(args.root_prefix, args.storage_path, table_id)
        print(f"  region_prefix  = {region_prefix}")
        print(f"  manifest_prefix= {manifest_prefix}")
        print(f"  table_dir      = {table_dir}")

        # Strict prefix check
        for pfx in [region_prefix, manifest_prefix]:
            if args.root_prefix not in pfx:
                fail(f"Prefix does not contain root-prefix '{args.root_prefix}': {pfx}")

        # ---- 5. Backup manifest objects from S3 --------------------------
        print(f"\n[{now_iso()}] listing & backing up manifest objects...")
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
            data = s3_get_object(transport, args.bucket, relative_key)
            local_path.write_bytes(data)
        print(f"  backup complete: {len(manifest_objects)} files")

        # ---- 6. Determine seed (delta-only for fresh table) --------------
        print(f"\n[{now_iso()}] determining seed source...")
        use_delta_seed = True  # Fresh table: use delta-only seed
        seed_delta_dir = backup_dir

        # Verify backup contains uncompressed .json deltas
        delta_files = sorted(
            p for p in backup_dir.iterdir()
            if p.is_file() and p.name.endswith(".json")
        )
        gz_files = [p for p in backup_dir.iterdir()
                    if p.is_file() and p.name.endswith(".json.gz")]
        if gz_files:
            fail(
                f"gzip-compressed delta found ({gz_files[0].name}) — "
                f".json.gz not supported. "
                f"Only uncompressed .json delta files are accepted."
            )
        if not delta_files:
            fail(
                f"No uncompressed .json delta files found in {backup_dir}. "
                f"Cannot construct seed manifest without deltas."
            )
        print(f"  using {len(delta_files)} delta file(s) as seed")

        # ---- 7. Run Rust readable SST fixture generator ------------------
        print(f"\n[{now_iso()}] running Rust readable SST fixture generator...")
        generated_dir = out_dir / "generated"
        generated_dir.mkdir(parents=True, exist_ok=True)

        if args.generator_bin:
            gen_cmd_prefix = args.generator_bin.split()
        else:
            gen_cmd_prefix = [
                "cargo", "run", "-p", "cmd",
                "--bin", "gc_readable_sst_fixture", "--",
            ]

        gen_args: List[str] = [
            "--out-dir", str(generated_dir),
            "--region-id", str(region_id),
            "--table-dir", str(table_dir),
            "--sst-count", str(args.sst_count),
            "--rows-per-sst", str(args.rows_per_sst),
            "--row-group-size", str(args.row_group_size),
            "--checkpoint-version", str(target_checkpoint_version),
            "--seed-delta-dir", str(seed_delta_dir),
        ]
        if args.skip_readback:
            gen_args.append("--skip-readback")
        if args.allow_large:
            gen_args.append("--allow-large")

        # Dry-run
        print(f"  dry-run...")
        dry_cmd = gen_cmd_prefix + gen_args + ["--dry-run"]
        dry_result = subprocess.run(dry_cmd, capture_output=True, text=True,
                                    cwd=os.path.dirname(_SCRIPT_DIR))
        write_text(str(out_dir / "generator-dry-run-stdout.txt"), dry_result.stdout)
        write_text(str(out_dir / "generator-dry-run-stderr.txt"), dry_result.stderr)
        if dry_result.returncode != 0:
            fail(f"Generator dry-run failed (exit {dry_result.returncode}):\n{dry_result.stderr}")
        print(dry_result.stdout)

        # Real
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
            print(f"  generated file_count: {gen_summary.get('file_count', '?')}")
            if gen_summary.get("readback_passed") is False:
                fail("Generator readback validation failed; aborting")

        # Read files.jsonl for later verification
        files_jsonl = generated_dir / "files.jsonl"
        if not files_jsonl.exists():
            fail(f"files.jsonl not found in generated dir: {files_jsonl}")

        # ---- 8. Baseline cluster capture ---------------------------------
        print(f"\n[{now_iso()}] capturing cluster baseline...")
        pods_before = kubectl_json(["get", "pod", "-n", args.namespace])
        write_json(str(out_dir / "pods-before.json"), pods_before)
        cluster_before = kubectl_json([
            "get", "greptimedbcluster", args.cluster_name, "-n", args.namespace
        ])
        write_json(str(out_dir / "cluster-before.json"), cluster_before)

        # ---- 9. Scale datanode to 0 --------------------------------------
        print(f"\n[{now_iso()}] scaling datanode StatefulSet to 0 (LAB-ONLY)...")
        subprocess.run([
            "kubectl", "scale", "statefulset", sts_name,
            "-n", args.namespace, "--replicas=0",
        ], check=True)
        datanode_scaled_down = True
        print(f"  scaled {sts_name} -> 0")

        label = f"app.greptime.io/component={args.cluster_name}-datanode"
        deadline = time.time() + 120
        while time.time() < deadline:
            if kubectl_pod_name_optional(label, args.namespace):
                time.sleep(2)
            else:
                break
        else:
            fail("datanode pod did not terminate within 120s")
        print(f"  datanode offline")

        # Pre-upload S3 counts
        pre_upload_counts = count_s3_objects(transport, args.bucket, region_prefix)
        write_json(str(out_dir / "s3-counts-before-upload.json"), pre_upload_counts)
        print(f"  s3 before upload: {pre_upload_counts}")

        # ---- 10. Upload generated .parquet files -------------------------
        print(f"\n[{now_iso()}] uploading generated SST .parquet files...")
        obj_store_dir = generated_dir / "object-store"
        uploaded_parquet = 0
        expected_prefix = region_prefix

        for parquet_file in sorted(obj_store_dir.rglob("*.parquet")):
            # Compute the S3 object key from the relative path inside object-store
            rel_key = str(parquet_file.relative_to(obj_store_dir))
            # The object-store root contains the full table_dir path
            s3_key = rel_key

            # Strict prefix check
            if not s3_key.startswith(expected_prefix):
                fail(
                    f"Generated .parquet path '{s3_key}' does not start with "
                    f"expected region prefix '{expected_prefix}'"
                )

            body = parquet_file.read_bytes()
            transport.put_object(args.bucket, s3_key, body)
            uploaded_parquet += 1
            print(f"  PUT {s3_key} ({len(body)} bytes)")

        print(f"  uploaded {uploaded_parquet} SST parquet files")
        if uploaded_parquet != args.sst_count:
            fail(
                f"uploaded parquet count {uploaded_parquet} != requested sst-count {args.sst_count}"
            )
        elapsed["upload_sst"] = time.perf_counter() - gen_start  # rough

        # ---- 11. PUT generated checkpoint + _last_checkpoint -------------
        print(f"\n[{now_iso()}] PUTting generated checkpoint...")
        manifest_gen_dir = generated_dir / "manifest"
        ck_file = manifest_gen_dir / f"{target_checkpoint_version:020}.checkpoint"
        if not ck_file.exists():
            fail(f"Generated checkpoint not found: {ck_file}")

        # PUT checkpoint first
        ck_bytes = ck_file.read_bytes()
        ck_s3_key = manifest_prefix + f"{target_checkpoint_version:020}.checkpoint"
        transport.put_object(args.bucket, ck_s3_key, ck_bytes)
        print(f"  PUT {ck_s3_key} ({len(ck_bytes)} bytes)")

        # PUT _last_checkpoint AFTER checkpoint
        last_ck_file = manifest_gen_dir / "_last_checkpoint"
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
            "uploaded_parquet": uploaded_parquet,
            "target_checkpoint_version": target_checkpoint_version,
            "max_existing_manifest_version": max_existing_manifest_version,
        })

        # Post-upload S3 counts
        post_upload_counts = count_s3_objects(transport, args.bucket, region_prefix)
        write_json(str(out_dir / "s3-counts-after-upload.json"), post_upload_counts)
        print(f"  s3 after upload: {post_upload_counts}")

        # ---- 12. Scale datanode back to 1 --------------------------------
        print(f"\n[{now_iso()}] scaling datanode StatefulSet back to 1...")
        scale_start = time.perf_counter()
        subprocess.run([
            "kubectl", "scale", "statefulset", sts_name,
            "-n", args.namespace, "--replicas=1",
        ], check=True)
        datanode_scaled_down = False
        print(f"  scaled {sts_name} -> 1")

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

        # ---- 13. Settle + SQL readback validation ------------------------
        print(f"\n[{now_iso()}] settling for {args.settle_seconds}s...")
        time.sleep(args.settle_seconds)

        expected_rows = args.sst_count * args.rows_per_sst
        print(f"\n[{now_iso()}] SQL SELECT COUNT(*) from table (expecting {expected_rows})...")
        code, text = sql_query(args.frontend_local_port,
                                f"SELECT COUNT(*) FROM {args.table}",
                                args.http_timeout)
        write_json(str(out_dir / "sql-select-count.json"),
                   {"sql": f"SELECT COUNT(*) FROM {args.table}",
                    "status_code": code, "response_text": text})
        if code != 200:
            fail(f"SELECT COUNT(*) failed HTTP {code}: {text[:500]}")
        resp = json.loads(text)
        count_rows = (resp.get("output", [{}])[0]
                      .get("records", {}).get("rows", []))
        actual_count = int(count_rows[0][0]) if count_rows else -1
        print(f"  COUNT(*) = {actual_count}")
        if actual_count != expected_rows:
            print(
                f"FAIL: COUNT(*) {actual_count} != expected {expected_rows}. "
                f"The generated readable SST checkpoint did not produce the expected SQL rows.",
                file=sys.stderr,
            )
            gate_status = "failed"

        # Query region_statistics
        print(f"\n[{now_iso()}] querying region statistics...")
        region_stats = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-after-swap.json"), region_stats)

        def _extract_rows(resp: Dict) -> List[Dict[str, Any]]:
            rows = (resp.get("response", {}).get("output", [{}])[0]
                    .get("records", {}).get("rows", []))
            cols = ["region_id", "table_id", "region_number",
                    "manifest_size", "sst_num", "sst_size", "memtable_size"]
            return [dict(zip(cols, r)) for r in rows]

        rs_rows = _extract_rows(region_stats)
        found_ids = {int(r["region_id"]) for r in rs_rows}
        if region_id not in found_ids:
            fail(f"region {region_id} not found in region_statistics")

        # ---- 14. Fast GC -------------------------------------------------
        fast_gc_elapsed = 0.0
        after_fast_gc = dict(post_upload_counts)
        if args.skip_fast_gc:
            print(f"\n[{now_iso()}] skipping fast GC (--skip-fast-gc)")
        else:
            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}) (fast)...")
            gc_result, fast_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id})",
                                  args.http_timeout))
            elapsed["fast_gc"] = fast_gc_elapsed
            code2, text2 = gc_result
            write_json(str(out_dir / "sql-fast-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id})",
                        "status_code": code2, "response_text": text2})
            if code2 != 200:
                fail(f"fast GC failed HTTP {code2}: {text2[:500]}")
            print(f"  fast GC completed in {fast_gc_elapsed:.2f}s")

            after_fast_gc = count_s3_objects(transport, args.bucket, region_prefix)
            write_json(str(out_dir / "s3-counts-after-fast-gc.json"), after_fast_gc)
            print(f"  s3 after fast GC: {after_fast_gc}")

        # ---- 15. Full GC -------------------------------------------------
        full_gc_elapsed = 0.0
        after_full_gc = dict(after_fast_gc)
        if args.skip_full_gc:
            print(f"\n[{now_iso()}] skipping full GC (--skip-full-gc)")
        else:
            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id}, true) (full)...")
            full_result, full_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id}, true)",
                                  args.http_timeout))
            elapsed["full_gc"] = full_gc_elapsed
            code3, text3 = full_result
            write_json(str(out_dir / "sql-full-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id}, true)",
                        "status_code": code3, "response_text": text3})
            if code3 != 200:
                fail(f"full GC failed HTTP {code3}: {text3[:500]}")
            print(f"  full GC completed in {full_gc_elapsed:.2f}s")

            after_full_gc = count_s3_objects(transport, args.bucket, region_prefix)
            write_json(str(out_dir / "s3-counts-after-full-gc.json"), after_full_gc)
            print(f"  s3 after full GC: {after_full_gc}")

            # Gate: parquet count must not decrease below generated count
            comparison_base = after_fast_gc if not args.skip_fast_gc else post_upload_counts
            generated_parquet = uploaded_parquet  # .parquet files we uploaded
            if after_full_gc["parquet"] < generated_parquet:
                print(
                    f"\n[{now_iso()}] FAIL: generated parquet count {generated_parquet} "
                    f"but only {after_full_gc['parquet']} remain after full GC. "
                    f"Some readable SSTs may have been deleted!",
                    file=sys.stderr,
                )
                gate_status = "failed"

        # ---- 16. HEAD each generated object to verify survival ------------
        print(f"\n[{now_iso()}] verifying generated SST objects survive GC...")
        survived = 0
        missing = 0
        with open(files_jsonl, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ref = json.loads(line)
                obj_path = ref.get("object_path", "")
                if obj_path:
                    s3_key = obj_path
                else:
                    # Fallback: construct from file_id + region_prefix
                    file_id = ref["file_id"]
                    s3_key = f"{region_prefix}{file_id}.parquet"
                if s3_head_object(transport, args.bucket, s3_key):
                    survived += 1
                else:
                    missing += 1
                    print(f"  MISSING: {s3_key}", file=sys.stderr)

        write_json(str(out_dir / "object-survival-verification.json"), {
            "total": survived + missing,
            "survived": survived,
            "missing": missing,
        })
        print(f"  object survival: {survived}/{survived + missing} present, "
              f"{missing}/{survived + missing} missing")

        if missing > 0:
            print(
                f"\n[{now_iso()}] FAIL: {missing} generated SST objects were deleted!",
                file=sys.stderr,
            )
            gate_status = "failed"

        # ---- 17. Cluster after + logs ------------------------------------
        print(f"\n[{now_iso()}] capturing cluster after...")
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

        # ---- 18. Write evidence summary ----------------------------------
        concise_lines = [
            f"EVIDENCE_DIR {args.out}",
            f"TABLE {args.table}",
            f"TABLE_ID {table_id}",
            f"REGION_ID {region_id}",
            f"SST_COUNT {args.sst_count}",
            f"ROWS_PER_SST {args.rows_per_sst}",
            f"EXPECTED_ROWS {expected_rows}",
            f"ACTUAL_COUNT {actual_count if 'actual_count' in dir() else -1}",
            f"UPLOADED_PARQUET {uploaded_parquet}",
            f"SURVIVED {survived}",
            f"MISSING {missing}",
            f"GATE_STATUS {gate_status}",
            f"CLUSTER_PHASE {cluster_phase}",
        ]
        for key, val in elapsed.items():
            concise_lines.append(f"{key.upper()}_ELAPSED_SECONDS {val}")

        concise = "\n".join(concise_lines) + "\n"
        write_text(str(out_dir / "concise-summary.txt"), concise)
        print(f"\n[{now_iso()}] evidence written to {out_dir}/")
        print(concise)

        if gate_status == "failed":
            print(f"\n[{now_iso()}] gate_status=failed; exiting with code 1")
            sys.exit(1)

    finally:
        if args.execute and datanode_scaled_down:
            print(
                f"\n[{now_iso()}] cleanup: scaling {sts_name} back to 1...",
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
