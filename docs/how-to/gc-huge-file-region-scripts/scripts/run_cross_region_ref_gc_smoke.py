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
run_cross_region_ref_gc_smoke.py
Test cross-region-reference GC smoke using a real two-partition table.

Flow:
1. Create a fresh two-partition table (PARTITION ON COLUMNS host).
2. Insert rows for both partitions → flush → get two region IDs.
3. Backup both region manifest objects from S3.
4. Run Rust cross-region fixture generator to produce coordinated synthetic
   checkpoints: region A has protected overlap X and unprotected Y in
   removed_files; region B has X active with FileMeta.region_id=A.
5. Write tiny placeholder .parquet objects for X/Y under region A's prefix.
6. Scale datanode to 0, PUT both region checkpoints then both _last_checkpoint.
7. Scale datanode to 1, run fast GC on region A.
8. Verify: protected X objects must survive; unprotected Y may be deleted.
   Protected deletion = FAIL. Unprotected non-deletion = WARN.

Placeholder parquet objects are NOT readable SSTs; no reads/compaction.

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
        description="Cross-region reference GC smoke harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Required
    p.add_argument("--overlap-count", type=int, default=10,
                   help="Number of protected overlap FileMeta (X, default: 10)")
    p.add_argument("--unprotected-count", type=int, default=10,
                   help="Number of unprotected FileMeta (Y, default: 10)")
    p.add_argument("--out", required=True,
                   help="Output directory for evidence files")

    # Cluster
    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--table", default="gc_hf_xref_smoke")
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
                   help="Safety override for large counts")
    g.add_argument("--skip-fast-gc", action="store_true")
    g.add_argument("--forbid-table-id", type=int, action="append", default=None,
                   help="Forbid a table_id (repeatable; default includes 1035)")
    g.add_argument("--checkpoint-version", type=int, default=1_000_000,
                   help="Minimum checkpoint version to use (default: 1000000; "
                        "the harness also raises this to cover existing manifest versions)")
    g.add_argument("--generator-bin", default=None,
                   help="Path or command for Rust generator "
                        "(default: cargo run -p cmd --bin gc_cross_region_ref_fixture --)")
    g.add_argument("--placeholder-object-bytes", type=int, default=1,
                   help="Size in bytes of each placeholder .parquet (default: 1)")

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


def prom_query(port: int, query: str) -> Tuple[int, str]:
    url = f"http://127.0.0.1:{port}/v1/prometheus/api/v1/query"
    params = urllib.parse.urlencode({"query": query})
    full_url = f"{url}?{params}"
    with urllib.request.urlopen(full_url, timeout=30) as resp:
        return resp.status, resp.read().decode(errors="replace")


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


def kubectl_pod_name(label: str, namespace: str) -> str:
    name = kubectl_pod_name_optional(label, namespace)
    if not name:
        fail(f"No pod found for label '{label}' in namespace '{namespace}'")
    assert name is not None
    return name


def kubectl_logs(pod: str, namespace: str, since: str,
                 container: Optional[str] = None) -> str:
    cmd = ["kubectl", "logs", "-n", namespace, pod, f"--since={since}"]
    if container:
        cmd.extend(["-c", container])
    return subprocess.check_output(cmd, text=True)


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


# ---- Proof helpers for post-GC verification -------------------------------
def verify_object_survival(
    transport: S3Transport,
    bucket: str,
    region_prefix: str,
    objects_jsonl: Path,
    expected_kind: str,
    label: str,
) -> Dict[str, Any]:
    """Check each object in objects.jsonl of given kind. Returns summary."""
    total = 0
    present = 0
    missing = 0
    with open(objects_jsonl, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ref = json.loads(line)
            if ref.get("kind") != expected_kind:
                continue
            total += 1
            file_id = ref["file_id"]
            key = f"{region_prefix}{file_id}.parquet"
            if s3_head_object(transport, bucket, key):
                present += 1
            else:
                missing += 1
    result = {
        "label": label,
        "kind": expected_kind,
        "total": total,
        "present": present,
        "missing": missing,
    }
    print(f"  {label}: {present}/{total} present, {missing}/{total} missing")
    return result


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
    print(f"Overlap (X):   {args.overlap_count}")
    print(f"Unprotected(Y):{args.unprotected_count}")
    print(f"Out dir:       {args.out}")
    print(f"Execute:       {args.execute}")
    print(f"Create table:  {args.create_table}")
    print(f"Forbid IDs:    {args.forbid_table_id or [1035]}")
    print()
    print("Planned steps:")
    print("  1. port-forward frontend/monitor/minio")
    print("  2. create two-partition table (if --create-table)")
    print("  3. insert rows for both partitions + flush")
    print("  4. query two region IDs; validate against forbid list")
    print("  5. backup both region manifest objects from S3")
    print("  6. run Rust cross-region fixture generator (dry-run then real)")
    print("  7. baseline cluster capture")
    print("  8. scale datanode → 0")
    print("  9. PUT placeholder .parquet objects for X/Y under region A")
    print(" 10. PUT both region checkpoints, then both _last_checkpoint")
    print(" 11. scale datanode → 1, wait Ready")
    print(" 12. verify both regions in region_statistics")
    print(" 13. fast GC on region A")
    print(" 14. verify: protected objects survive; unprotected may be deleted")
    print(" 15. write evidence + concise summary")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    args = build_parser().parse_args()

    if args.forbid_table_id is None:
        args.forbid_table_id = [1035]
    forbid_table_names = {"gc_hf_test_c"}

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Safety cap
    total_objects = args.overlap_count + args.unprotected_count
    if total_objects > 100 and not args.allow_large:
        fail(
            f"Total objects ({total_objects}) exceeds 100. Use --allow-large to override."
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

        # ---- 2. Create two-partition table -------------------------------
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
                    f"host STRING, v BIGINT, x DOUBLE, label STRING, "
                    f"ts TIMESTAMP TIME INDEX)"
                    f" PARTITION ON COLUMNS (host) ("
                    f" host < 'm',"
                    f" host >= 'm'"
                    f")"
                    f" ENGINE=mito WITH ("
                    f"'append_mode' = 'true', "
                    f"'compaction.type' = 'twcs', "
                    f"'compaction.twcs.trigger_file_num' = '1000000000')"
                )
                print(f"  table absent; creating with 2 partitions...")
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

        if table_id in (args.forbid_table_id or []):
            fail(f"table_id={table_id} is in forbidden list {args.forbid_table_id}")
        if args.table in forbid_table_names:
            fail(f"table name '{args.table}' is forbidden (protected C1 table)")

        # Get both partition/region IDs
        code, text = sql_query(args.frontend_local_port,
                               f"SELECT greptime_partition_id FROM information_schema.partitions "
                               f"WHERE table_name='{args.table}' ORDER BY greptime_partition_id",
                               args.http_timeout)
        sql_part = json.loads(text)
        part_rows = sql_part.get("output", [{}])[0].get("records", {}).get("rows", [])
        if len(part_rows) < 2:
            fail(
                f"Expected at least 2 partitions for table '{args.table}', "
                f"got {len(part_rows)}. Did you create with PARTITION ON COLUMNS?"
            )
        region_id_a = int(part_rows[0][0])
        region_id_b = int(part_rows[1][0])
        print(f"  region_id A = {region_id_a}")
        print(f"  region_id B = {region_id_b}")

        # Compute prefixes
        prefix_a = region_prefix_for(args.root_prefix, args.storage_path, region_id_a)
        prefix_b = region_prefix_for(args.root_prefix, args.storage_path, region_id_b)
        manifest_prefix_a = prefix_a + "manifest/"
        manifest_prefix_b = prefix_b + "manifest/"
        print(f"  prefix A = {prefix_a}")
        print(f"  prefix B = {prefix_b}")

        for pfx in [prefix_a, prefix_b]:
            if "gc-hf-lab" not in pfx:
                fail(f"prefix does not contain 'gc-hf-lab': {pfx}")

        # ---- 4. Seed rows + flush ----------------------------------------
        print(f"\n[{now_iso()}] inserting seed rows for both partitions + flush...")
        seed_value = int(time.time() * 1000) % 1000000000
        for host_val in ["a", "z"]:  # a goes to partition 0 (< 'm'), z to partition 1 (>= 'm')
            seed_sql = (
                f"INSERT INTO {args.table} VALUES "
                f"('{host_val}', {seed_value}, {float(seed_value)}, 'xref_seed', now())"
            )
            code, text = sql_query(args.frontend_local_port, seed_sql, args.http_timeout)
            if code != 200:
                print(f"  WARNING: seed INSERT for host='{host_val}' HTTP {code}: {text[:200]}",
                      file=sys.stderr)
        code, text = sql_query(
            args.frontend_local_port,
            f"ADMIN FLUSH_TABLE('{args.table}')",
            args.http_timeout,
        )
        if code != 200:
            print(f"  WARNING: seed FLUSH HTTP {code}: {text[:200]}", file=sys.stderr)
        print(f"  seed rows inserted + flushed")

        # ---- 5. S3 manifest backup for both regions ----------------------
        max_existing_manifest_version = 0
        for label, pfx, mpfx in [
            ("A", prefix_a, manifest_prefix_a),
            ("B", prefix_b, manifest_prefix_b),
        ]:
            print(f"\n[{now_iso()}] backing up region {label} manifest objects...")
            manifest_objects = list_s3_objects(transport, args.bucket, mpfx)
            write_json(str(out_dir / f"manifest-objects-{label}-before.json"), manifest_objects)
            print(f"  found {len(manifest_objects)} manifest object(s) for region {label}")
            max_existing_manifest_version = max(
                max_existing_manifest_version,
                max_manifest_object_version(manifest_objects),
            )

            backup_dir = out_dir / f"manifest-backup-{label.lower()}"
            backup_dir.mkdir(parents=True, exist_ok=True)
            for obj in manifest_objects:
                relative_key = obj["key"]
                if relative_key.startswith(mpfx):
                    rel_path = relative_key[len(mpfx):]
                else:
                    rel_path = os.path.basename(relative_key)
                local_path = backup_dir / rel_path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                data = s3_get_object(transport, args.bucket, relative_key)
                local_path.write_bytes(data)
            print(f"  backup complete: {len(manifest_objects)} files")

        target_checkpoint_version = max(
            args.checkpoint_version,
            max_existing_manifest_version,
        )
        if target_checkpoint_version != args.checkpoint_version:
            print(
                f"  using checkpoint version {target_checkpoint_version} "
                f"to cover existing manifest versions up to {max_existing_manifest_version}"
            )

        # ---- 6. Find seed for each region (delta-only if no _last_checkpoint)
        use_delta_seed = True  # By default for fresh C2-style tables
        backup_dir_a = out_dir / "manifest-backup-a"
        backup_dir_b = out_dir / "manifest-backup-b"

        # Check if _last_checkpoint exists for region A; if so, could use checkpoint seed
        last_ck_a = s3_get_object_optional(transport, args.bucket, manifest_prefix_a + "_last_checkpoint")
        last_ck_b = s3_get_object_optional(transport, args.bucket, manifest_prefix_b + "_last_checkpoint")

        has_checkpoint_seed = last_ck_a is not None and last_ck_b is not None
        if has_checkpoint_seed:
            print(f"\n[{now_iso()}] checkpoint seed available for both regions")
            # Use checkpoint seed path (not implemented in this version; use delta)
            # For now, fall through to delta even if checkpoint exists,
            # since the fixture generator supports both.
            pass
        else:
            print(f"\n[{now_iso()}] _last_checkpoint missing in at least one region → delta-only seed")

        use_delta_seed = True

        # ---- 7. Run Rust cross-region fixture generator ------------------
        print(f"\n[{now_iso()}] running Rust cross-region fixture generator...")
        generated_dir = out_dir / "generated"
        generated_dir.mkdir(parents=True, exist_ok=True)

        if args.generator_bin:
            gen_cmd_prefix = args.generator_bin.split()
        else:
            gen_cmd_prefix = [
                "cargo", "run", "-p", "cmd",
                "--bin", "gc_cross_region_ref_fixture", "--"
            ]

        gen_args: List[str] = [
            "--region-a-id", str(region_id_a),
            "--region-b-id", str(region_id_b),
            "--out-dir", str(generated_dir),
            "--overlap-count", str(args.overlap_count),
            "--unprotected-count", str(args.unprotected_count),
            "--version", str(target_checkpoint_version),
        ]

        if use_delta_seed:
            gen_args.extend([
                "--seed-a-delta-dir", str(backup_dir_a),
                "--seed-b-delta-dir", str(backup_dir_b),
            ])
        # else: would use --seed-a-checkpoint / --seed-b-checkpoint

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

        # ---- 8. Baseline cluster capture ---------------------------------
        print(f"\n[{now_iso()}] capturing cluster baseline...")
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
        print(f"  scaled {sts_name} → 0")

        datanode_label = f"app.greptime.io/component={args.cluster_name}-datanode"
        deadline = time.time() + 120
        while time.time() < deadline:
            if kubectl_pod_name_optional(datanode_label, args.namespace):
                time.sleep(2)
            else:
                break
        else:
            fail("datanode pod did not terminate within 120s")
        print(f"  datanode offline")

        # ---- 10. PUT placeholder .parquet objects under region A ---------
        print(f"\n[{now_iso()}] materializing placeholder .parquet objects under region A...")
        objects_jsonl = generated_dir / "objects.jsonl"
        if not objects_jsonl.exists():
            fail(f"objects.jsonl not found: {objects_jsonl}")

        placeholder = bytes(args.placeholder_object_bytes)
        written = 0
        t0 = time.perf_counter()
        with open(objects_jsonl, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ref = json.loads(line)
                file_id = ref["file_id"]
                parquet_key = f"{prefix_a}{file_id}.parquet"
                transport.put_object(args.bucket, parquet_key, placeholder)
                written += 1
        mat_elapsed = time.perf_counter() - t0
        elapsed["materialize"] = mat_elapsed
        print(f"  materialized {written} placeholder objects in {mat_elapsed:.2f}s")

        pre_gc_obj_counts = count_s3_objects(transport, args.bucket, prefix_a)
        write_json(str(out_dir / "s3-counts-a-before-gc.json"), pre_gc_obj_counts)
        print(f"  region A objects before GC: {pre_gc_obj_counts}")

        # ---- 11. PUT both region checkpoints + _last_checkpoint ----------
        for region_label, pfx_m in [("A", manifest_prefix_a), ("B", manifest_prefix_b)]:
            region_sub = f"region-{region_label.lower()}"
            ck_file = generated_dir / region_sub / f"{target_checkpoint_version:020}.checkpoint"
            lc_file = generated_dir / region_sub / "_last_checkpoint"

            if not ck_file.exists():
                fail(f"Checkpoint not found: {ck_file}")
            ck_bytes = ck_file.read_bytes()
            ck_s3_key = pfx_m + f"{target_checkpoint_version:020}.checkpoint"
            transport.put_object(args.bucket, ck_s3_key, ck_bytes)
            print(f"  PUT {ck_s3_key} ({len(ck_bytes)} bytes)")

            if not lc_file.exists():
                fail(f"_last_checkpoint not found: {lc_file}")
            lc_bytes = lc_file.read_bytes()
            lc_s3_key = pfx_m + "_last_checkpoint"
            transport.put_object(args.bucket, lc_s3_key, lc_bytes)
            print(f"  PUT {lc_s3_key} ({len(lc_bytes)} bytes)")

        write_json(str(out_dir / "put-manifest-summary.json"), {
            "region_a_id": region_id_a,
            "region_b_id": region_id_b,
            "checkpoint_version": target_checkpoint_version,
            "requested_checkpoint_version": args.checkpoint_version,
            "max_existing_manifest_version": max_existing_manifest_version,
            "materialized_objects": written,
        })

        # ---- 12. Scale datanode back to 1 --------------------------------
        print(f"\n[{now_iso()}] scaling datanode StatefulSet back to 1...")
        scale_start = time.perf_counter()
        subprocess.run([
            "kubectl", "scale", "statefulset", sts_name,
            "-n", args.namespace, "--replicas=1",
        ], check=True)
        datanode_scaled_down = False

        deadline = time.time() + 300
        pod_ready = False
        while time.time() < deadline:
            pod_name = kubectl_pod_name_optional(datanode_label, args.namespace)
            if not pod_name:
                time.sleep(5)
                continue
            pod_json = kubectl_json(["get", "pod", "-n", args.namespace, pod_name])
            conditions = pod_json.get("status", {}).get("conditions", [])
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

        print(f"\n[{now_iso()}] querying region statistics...")
        region_stats = query_region_stats(args.frontend_local_port, table_id, args.http_timeout)
        write_json(str(out_dir / "region-stats-after-swap.json"), region_stats)

        def _extract_all_rows(resp: Dict) -> List[Dict[str, Any]]:
            rows = (resp.get("response", {}).get("output", [{}])[0]
                    .get("records", {}).get("rows", []))
            cols = ["region_id", "table_id", "region_number",
                    "manifest_size", "sst_num", "sst_size", "memtable_size"]
            return [dict(zip(cols, r)) for r in rows]

        rs_rows = _extract_all_rows(region_stats)
        found_ids = {int(r["region_id"]) for r in rs_rows}
        if region_id_a not in found_ids:
            print(f"WARNING: region A ({region_id_a}) not in region_statistics", file=sys.stderr)
        if region_id_b not in found_ids:
            print(f"WARNING: region B ({region_id_b}) not in region_statistics", file=sys.stderr)

        # ---- 14. Fast GC on region A -------------------------------------
        if args.skip_fast_gc:
            print(f"\n[{now_iso()}] skipping fast GC (--skip-fast-gc)")
        else:
            print(f"\n[{now_iso()}] running ADMIN GC_REGIONS({region_id_a}) (fast)...")
            gc_result, fast_gc_elapsed = timed(
                lambda: sql_query(args.frontend_local_port,
                                  f"ADMIN GC_REGIONS({region_id_a})",
                                  args.http_timeout))
            elapsed["fast_gc"] = fast_gc_elapsed
            code2, text2 = gc_result
            write_json(str(out_dir / "sql-fast-gc.json"),
                       {"sql": f"ADMIN GC_REGIONS({region_id_a})",
                        "status_code": code2, "response_text": text2})
            if code2 != 200:
                fail(f"fast GC failed HTTP {code2}: {text2[:500]}")
            print(f"  fast GC completed in {fast_gc_elapsed:.2f}s")

        # ---- 15. Post-GC verification ------------------------------------
        print(f"\n[{now_iso()}] post-GC S3 counts for region A...")
        post_gc_obj_counts = count_s3_objects(transport, args.bucket, prefix_a)
        write_json(str(out_dir / "s3-counts-a-after-gc.json"), post_gc_obj_counts)
        print(f"  region A objects after GC: {post_gc_obj_counts}")

        # Verify individual objects: protected must survive, unprotected may be deleted
        prot_result = verify_object_survival(
            transport, args.bucket, prefix_a, objects_jsonl,
            "protected", "protected-objects",
        )
        unprot_result = verify_object_survival(
            transport, args.bucket, prefix_a, objects_jsonl,
            "unprotected", "unprotected-objects",
        )

        write_json(str(out_dir / "object-survival-verification.json"), {
            "protected": prot_result,
            "unprotected": unprot_result,
        })

        # Gate: protected deletion = FAIL
        if prot_result["missing"] > 0:
            print(
                f"\n[{now_iso()}] FAIL: {prot_result['missing']} protected objects "
                f"were deleted by GC! Cross-region ref protection may be broken.",
                file=sys.stderr,
            )
            gate_status = "failed"

        # Unprotected deletion = expected but not required for first smoke
        if unprot_result["missing"] > 0:
            print(
                f"  unprotected objects deleted: {unprot_result['missing']}/{unprot_result['total']} "
                f"(expected if GC eligible)"
            )
        else:
            print(
                f"  WARNING: 0 unprotected objects deleted (may not be eligible yet; "
                f"not necessarily a failure on first smoke)",
                file=sys.stderr,
            )

        # ---- 16. Cluster after + logs ------------------------------------
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

        # ---- 17. Write evidence summary ----------------------------------
        concise_lines = [
            f"EVIDENCE_DIR {args.out}",
            f"TABLE {args.table}",
            f"TABLE_ID {table_id}",
            f"REGION_ID_A {region_id_a}",
            f"REGION_ID_B {region_id_b}",
            f"OVERLAP_COUNT {args.overlap_count}",
            f"UNPROTECTED_COUNT {args.unprotected_count}",
            f"GATE_STATUS {gate_status}",
            f"PROTECTED_PRESENT {prot_result['present']}/{prot_result['total']}",
            f"PROTECTED_MISSING {prot_result['missing']}/{prot_result['total']}",
            f"UNPROTECTED_PRESENT {unprot_result['present']}/{unprot_result['total']}",
            f"UNPROTECTED_MISSING {unprot_result['missing']}/{unprot_result['total']}",
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
