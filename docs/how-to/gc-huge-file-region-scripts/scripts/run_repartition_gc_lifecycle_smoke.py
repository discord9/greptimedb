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
GC + real repartition lifecycle deletion-candidate smoke.

Dry-run (default) only prints the execution plan. Pass --execute to run against a
live cluster.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from run_repartition_gc_correctness_smoke import (  # type: ignore[import-not-found]
    S3Transport,
    backup_region_manifests,
    count_s3_objects,
    fail,
    kubectl_json,
    now_iso,
    partition_description_has_value,
    port_forward,
    query_partitions,
    query_table_id,
    region_prefix_for,
    s3_head_object,
    sql_query,
    sql_query_ok_rows,
    wait_for_port,
    write_json,
    write_text,
)


Key = Tuple[str, str]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="GC + repartition lifecycle deletion-candidate smoke harness.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--out", required=True, help="Output directory for evidence files")

    g = p.add_argument_group("cluster")
    g.add_argument("--namespace", default="gc-stress-test")
    g.add_argument("--db", default="metrics_gc_repart_lifecycle_smoke")
    g.add_argument("--physical-table", default="greptime_physical_table")
    g.add_argument("--table", default="gc_repart_lifecycle_logical")
    g.add_argument("--frontend-service", default="gc-stress-greptimedb-frontend")
    g.add_argument("--monitor-service", default="gc-stress-greptimedb-monitor-standalone")
    g.add_argument("--minio-service", default="minio")
    g.add_argument("--cluster-name", default="gc-stress-greptimedb")

    g = p.add_argument_group("ports")
    g.add_argument("--frontend-local-port", type=int, default=14000)
    g.add_argument("--frontend-remote-port", type=int, default=4000)
    g.add_argument("--monitor-local-port", type=int, default=15000)
    g.add_argument("--monitor-remote-port", type=int, default=4000)
    g.add_argument("--minio-local-port", type=int, default=19000)
    g.add_argument("--minio-remote-port", type=int, default=9000)

    g = p.add_argument_group("s3")
    g.add_argument("--s3-endpoint", default="http://127.0.0.1:19000")
    g.add_argument("--s3-access-key", default=os.environ.get("GC_STRESS_S3_ACCESS_KEY", "rootuser"))
    g.add_argument("--s3-secret-key", default=os.environ.get("GC_STRESS_S3_SECRET_KEY", ""),
                   help="S3 secret key, or set GC_STRESS_S3_SECRET_KEY")
    g.add_argument("--bucket", default="gc-stress-bucket")
    g.add_argument("--root-prefix", default="gc-hf-lab")
    g.add_argument("--storage-path", default=None,
                   help="Path component inside root-prefix (default: greptime/<db>)")

    g = p.add_argument_group("rate")
    g.add_argument("--pre-split-flushes", type=int, default=3)
    g.add_argument("--post-split-flushes", type=int, default=3)
    g.add_argument("--http-timeout", type=float, default=600.0)
    g.add_argument("--settle-seconds", type=int, default=30)
    g.add_argument("--split-poll-timeout", type=int, default=120)

    g = p.add_argument_group("flags")
    g.add_argument("--execute", action="store_true")
    g.add_argument("--create-table", action="store_true")
    g.add_argument("--skip-fast-gc", action="store_true")
    g.add_argument("--generator-bin", default="cargo run -p cmd --bin gc_region_manifest_summary --",
                   help="Path or command for Rust scanner")
    g.add_argument("--allow-inconclusive", action="store_true",
                    help="Do not fail before GC when no referenced removed candidates are found")
    g.add_argument("--candidate-retries", type=int, default=2,
                   help="Extra post-compact attempts when referenced_removed is empty")
    return p


def key_of(entry: Dict[str, Any]) -> Key:
    return str(entry.get("file_id", "")), "" if entry.get("index_version") is None else str(entry.get("index_version"))


def key_to_dict(key: Key) -> Dict[str, str]:
    return {"file_id": key[0], "index_version": key[1]}


def keys_to_dicts(keys: Set[Key]) -> List[Dict[str, str]]:
    return [key_to_dict(k) for k in sorted(keys)]


def child_is_left(partition: Dict[str, Any]) -> bool:
    desc = str(partition.get("partition_description", ""))
    return partition_description_has_value(desc, "g") and "<" in desc and not partition_description_has_value(desc, "m")


def child_is_right(partition: Dict[str, Any]) -> bool:
    desc = str(partition.get("partition_description", ""))
    return partition_description_has_value(desc, "g") and partition_description_has_value(desc, "m")


def source_partition_match(partition: Dict[str, Any]) -> bool:
    desc = str(partition.get("partition_description", ""))
    return partition_description_has_value(desc, "m") and "<" in desc and not partition_description_has_value(desc, "g")


def scanner_prefix(args: argparse.Namespace) -> List[str]:
    return shlex.split(args.generator_bin)


def run_scanner(args: argparse.Namespace, region_id: int, backup_dir: Path, output: Path) -> Dict[str, Any]:
    cmd = scanner_prefix(args) + ["--region-id", str(region_id), "--seed-delta-dir", str(backup_dir), "--output", str(output)]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(_SCRIPT_DIR))
    if result.returncode != 0:
        fail(f"Scanner failed for region {region_id}: {result.stderr}")
    return json.loads(output.read_text())


def scan_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "file_count": summary.get("file_count"),
        "removed_file_count": summary.get("removed_file_count"),
        "removed_index_count": summary.get("removed_index_count"),
        "removed_count": summary.get("removed_count"),
        "cross_region_file_count": summary.get("cross_region_file_count"),
        "file_region_id_counts": summary.get("file_region_id_counts", {}),
    }


def dry_run_plan(args: argparse.Namespace) -> None:
    print("=== DRY-RUN PLAN ===")
    print(f"DB:             {args.db}")
    print(f"Physical table: {args.physical_table}")
    print(f"Logical table:  {args.table}")
    print(f"Out dir:        {args.out}")
    print(f"Pre flushes:    {args.pre_split_flushes}")
    print(f"Post flushes:   {args.post_split_flushes}")
    print(f"Candidate retries: {args.candidate_retries}")
    print("Planned steps:")
    print("  1. create fresh metric physical table partitioned at host < 'm' / host >= 'm'")
    print("  2. create logical metric table on the physical table")
    print("  3. insert a_host + h_host batches and flush before split")
    print("  4. SPLIT PARTITION host < 'm' into host < 'g' and host >= 'g' AND host < 'm'")
    print("  5. identify reused source child and non-source destination child")
    print("  6. back up destination manifest and scan source-region active refs")
    print("  7. post-split insert only into reused source child, flushing each batch")
    print("  8. ADMIN COMPACT_REGION(source_region_id)")
    print("  9. scan source + destination manifests, compute R/S/D and referenced removed candidates")
    print(" 10. if referenced_removed is empty, retry insert/flush/compact/scan up to --candidate-retries")
    print(" 11. gate on referenced_removed unless --allow-inconclusive")
    print(" 12. ADMIN GC_REGIONS(source_region_id), HEAD referenced SSTs, verify SQL reads")


def ensure_fresh_tables(args: argparse.Namespace) -> None:
    code, text = sql_query(args.frontend_local_port, f"CREATE DATABASE IF NOT EXISTS {args.db}", args.http_timeout)
    if code != 200:
        fail(f"CREATE DATABASE failed HTTP {code}: {text[:500]}")
    rows = sql_query_ok_rows(args.frontend_local_port, (
        f"SELECT table_name FROM information_schema.tables WHERE table_schema='{args.db}' "
        f"AND table_name IN ('{args.physical_table}', '{args.table}')"
    ), args.http_timeout)
    if rows:
        fail(f"Tables already exist in {args.db}: {[r[0] for r in rows]}. Drop them first.")
    if not args.create_table:
        fail(f"Fresh tables are required; pass --create-table to create {args.db}.{args.physical_table}/{args.table}")

    physical_ddl = (
        f"CREATE TABLE {args.db}.{args.physical_table} ("
        f"`host` STRING NULL INVERTED INDEX, "
        f"greptime_timestamp TIMESTAMP(3) NOT NULL, "
        f"greptime_value DOUBLE NULL, "
        f"PRIMARY KEY (`host`), TIME INDEX (greptime_timestamp)) "
        f"PARTITION ON COLUMNS (`host`) (`host` < 'm', `host` >= 'm') "
        f"ENGINE = metric WITH ('physical_metric_table' = '', 'memtable.type' = 'partition_tree', "
        f"'sst_format' = 'flat', 'memtable.partition_tree.primary_key_encoding' = 'sparse', "
        f"'index.type' = 'inverted')"
    )
    code, text = sql_query(args.frontend_local_port, physical_ddl, args.http_timeout)
    if code != 200:
        fail(f"CREATE PHYSICAL TABLE failed HTTP {code}: {text[:500]}")
    logical_ddl = (
        f"CREATE TABLE {args.db}.{args.table} ("
        f"`host` STRING NULL, greptime_timestamp TIMESTAMP(3) NOT NULL, greptime_value DOUBLE NULL, "
        f"PRIMARY KEY (`host`), TIME INDEX (greptime_timestamp)) "
        f"ENGINE = metric WITH ('on_physical_table' = '{args.physical_table}')"
    )
    code, text = sql_query(args.frontend_local_port, logical_ddl, args.http_timeout)
    if code != 200:
        fail(f"CREATE LOGICAL TABLE failed HTTP {code}: {text[:500]}")


def insert_row(args: argparse.Namespace, host: str, ts: int, value: float) -> None:
    code, text = sql_query(args.frontend_local_port, (
        f"INSERT INTO {args.db}.{args.table} (host, greptime_timestamp, greptime_value) "
        f"VALUES ('{host}', {ts}, {value})"
    ), args.http_timeout)
    if code != 200:
        fail(f"INSERT {host}/{ts} failed HTTP {code}: {text[:500]}")


def flush_physical(args: argparse.Namespace) -> None:
    code, text = sql_query(args.frontend_local_port, f"ADMIN FLUSH_TABLE('{args.db}.{args.physical_table}')", args.http_timeout)
    if code != 200:
        fail(f"FLUSH physical table failed HTTP {code}: {text[:500]}")


def main() -> None:
    args = build_parser().parse_args()
    if args.candidate_retries < 0:
        fail("--candidate-retries must be non-negative")
    if args.storage_path is None:
        args.storage_path = f"greptime/{args.db}"
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    if not args.execute:
        dry_run_plan(args)
        print(f"\n[{now_iso()}] Dry-run complete. Use --execute to run.")
        return

    pf_procs: List[Tuple[subprocess.Popen, Any]] = []
    gate_status = "passed"
    try:
        for local, remote, service in [
            (args.frontend_local_port, args.frontend_remote_port, args.frontend_service),
            (args.monitor_local_port, args.monitor_remote_port, args.monitor_service),
            (args.minio_local_port, args.minio_remote_port, args.minio_service),
        ]:
            pf_procs.append(port_forward(local, remote, args.namespace, service, out_dir))
        for name, port in [("frontend", args.frontend_local_port), ("monitor", args.monitor_local_port), ("minio", args.minio_local_port)]:
            if not wait_for_port(port):
                fail(f"Port {port} ({name}) not ready after 30s")

        transport = S3Transport(args.s3_endpoint, args.s3_access_key, args.s3_secret_key)
        ensure_fresh_tables(args)
        table_id = query_table_id(args.frontend_local_port, args.db, args.physical_table, args.http_timeout)
        logical_table_id = query_table_id(args.frontend_local_port, args.db, args.table, args.http_timeout)

        base_ts = int(time.time() * 1000)
        for i in range(args.pre_split_flushes):
            insert_row(args, "a_host", base_ts + i * 10 + 1, float(i))
            insert_row(args, "h_host", base_ts + i * 10 + 2, float(i) + 0.5)
            flush_physical(args)

        before_partitions = query_partitions(args.frontend_local_port, args.db, args.physical_table, args.http_timeout)
        write_json(str(out_dir / "partitions-before.json"), before_partitions)
        source_matches = [p for p in before_partitions if source_partition_match(p)]
        if len(source_matches) != 1:
            fail(f"Expected exactly one source partition host < 'm', got {source_matches}")
        source_region_id = source_matches[0]["region_id"]
        source_prefix = region_prefix_for(args.root_prefix, args.storage_path, source_region_id)
        s3_before = count_s3_objects(transport, args.bucket, source_prefix)

        split_sql = (
            f"ALTER TABLE {args.db}.{args.physical_table} SPLIT PARTITION (`host` < 'm') "
            f"INTO (`host` < 'g', `host` >= 'g' AND `host` < 'm')"
        )
        code, text = sql_query(args.frontend_local_port, split_sql, args.http_timeout)
        write_json(str(out_dir / "sql-split-response.json"), {"sql": split_sql, "status_code": code, "response": text})
        if code != 200:
            fail(f"SPLIT PARTITION failed HTTP {code}: {text[:500]}")

        expected_pre = 2 * args.pre_split_flushes
        deadline = time.time() + args.split_poll_timeout
        after_partitions: List[Dict[str, Any]] = []
        while time.time() < deadline:
            after_partitions = query_partitions(args.frontend_local_port, args.db, args.physical_table, args.http_timeout)
            count_rows = sql_query_ok_rows(args.frontend_local_port, f"SELECT COUNT(*) FROM {args.db}.{args.table}", args.http_timeout)
            if len(after_partitions) == 3 and int(count_rows[0][0]) == expected_pre:
                break
            time.sleep(5)
        else:
            fail(f"Split did not complete within {args.split_poll_timeout}s; partitions={after_partitions}")
        write_json(str(out_dir / "partitions-after-split.json"), after_partitions)

        left = [p for p in after_partitions if child_is_left(p)]
        right = [p for p in after_partitions if child_is_right(p)]
        if len(left) != 1 or len(right) != 1:
            fail(f"Expected split children host<'g' and g<=host<'m', got left={left}, right={right}")
        children = [left[0], right[0]]
        dest_partitions = [p for p in children if p["region_id"] != source_region_id]
        reused = [p for p in children if p["region_id"] == source_region_id]
        if not dest_partitions or len(reused) != 1:
            fail(f"Expected one reused source child and at least one destination, children={children}, source={source_region_id}")
        dest_region_ids = sorted({p["region_id"] for p in dest_partitions})
        reused_host = "a_host" if child_is_left(reused[0]) else "h_host"
        write_json(str(out_dir / "split-child-partitions.json"), {
            "source_region_id": source_region_id,
            "reused_source_child": reused[0],
            "destination_partitions": dest_partitions,
            "destination_region_ids": dest_region_ids,
            "post_split_host": reused_host,
        })

        dest_initial: Dict[int, Dict[str, Any]] = {}
        for drid in dest_region_ids:
            dprefix = region_prefix_for(args.root_prefix, args.storage_path, drid)
            dbackup, _, _ = backup_region_manifests(transport, args.bucket, dprefix, drid, out_dir, f"dest-{drid}-after-split")
            summary = run_scanner(args, drid, dbackup, out_dir / f"manifest-summary-dest-{drid}-after-split.json")
            dest_initial[drid] = scan_summary(summary)
        write_json(str(out_dir / "destination-after-split-scan-summary.json"), dest_initial)

        for i in range(args.post_split_flushes):
            insert_row(args, reused_host, base_ts + 1000 + i, 100.0 + i)
            flush_physical(args)
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)

        expected_count = expected_pre + args.post_split_flushes
        compact_sql = f"ADMIN COMPACT_REGION({source_region_id})"
        compact_attempts: List[Dict[str, Any]] = []
        candidate_attempts: List[Dict[str, Any]] = []
        source_data_prefix = ""
        source_summary: Dict[str, Any] = {}
        dest_summaries: Dict[int, Dict[str, Any]] = {}
        source_removed: Set[Key] = set()
        source_active: Set[Key] = set()
        dest_active_source: Set[Key] = set()
        referenced_removed: Set[Key] = set()
        still_active_removed: Set[Key] = set()
        unreferenced_removed: Set[Key] = set()

        for attempt in range(args.candidate_retries + 1):
            if attempt > 0:
                insert_row(args, reused_host, base_ts + 2000 + attempt, 200.0 + attempt)
                expected_count += 1
                flush_physical(args)
            code, text = sql_query(args.frontend_local_port, compact_sql, args.http_timeout)
            compact_attempts.append({"attempt": attempt, "sql": compact_sql, "status_code": code, "response": text})
            write_json(str(out_dir / "sql-compact-source-attempts.json"), compact_attempts)
            if code != 200:
                fail(f"COMPACT_REGION source attempt {attempt} failed HTTP {code}: {text[:500]}")

            attempt_label = f"attempt-{attempt}"
            src_backup, _, src_manifest_prefix = backup_region_manifests(
                transport, args.bucket, source_prefix, source_region_id, out_dir, f"source-after-compact-{attempt_label}"
            )
            source_data_prefix = src_manifest_prefix.removesuffix("manifest/").removesuffix("data/manifest/")
            if not source_data_prefix.endswith("/"):
                source_data_prefix += "/"
            source_summary = run_scanner(
                args, source_region_id, src_backup, out_dir / f"manifest-summary-source-after-compact-{attempt_label}.json"
            )
            dest_summaries = {}
            dest_active_source = set()
            for drid in dest_region_ids:
                dprefix = region_prefix_for(args.root_prefix, args.storage_path, drid)
                dbackup, _, _ = backup_region_manifests(
                    transport, args.bucket, dprefix, drid, out_dir, f"dest-{drid}-after-compact-{attempt_label}"
                )
                summary = run_scanner(args, drid, dbackup, out_dir / f"manifest-summary-dest-{drid}-after-compact-{attempt_label}.json")
                dest_summaries[drid] = scan_summary(summary)
                dest_active_source.update(key_of(f) for f in summary.get("files", []) if f.get("file_region_id") == source_region_id)

            source_removed = {key_of(f) for f in source_summary.get("removed_files", []) if f.get("kind") == "File"}
            source_active = {key_of(f) for f in source_summary.get("files", [])}
            referenced_removed = source_removed & dest_active_source
            still_active_removed = referenced_removed & source_active
            unreferenced_removed = source_removed - dest_active_source - source_active
            attempt_summary = {
                "attempt": attempt,
                "retry_inserted": attempt > 0,
                "source_removed_count": len(source_removed),
                "source_active_count": len(source_active),
                "dest_active_source_count": len(dest_active_source),
                "referenced_removed_count": len(referenced_removed),
                "referenced_removed_still_active_in_source_count": len(still_active_removed),
                "unreferenced_removed_count": len(unreferenced_removed),
                "source_scan_summary": scan_summary(source_summary),
                "destination_scan_summaries": {str(k): v for k, v in dest_summaries.items()},
            }
            candidate_attempts.append(attempt_summary)
            write_json(str(out_dir / "candidate-attempts.json"), candidate_attempts)
            if still_active_removed:
                fail(f"referenced_removed overlaps source active files: {keys_to_dicts(still_active_removed)}")
            if referenced_removed:
                break
        write_json(str(out_dir / "candidate-sets.json"), {
            "source_removed_R": keys_to_dicts(source_removed),
            "source_active_S": keys_to_dicts(source_active),
            "dest_active_source_D": keys_to_dicts(dest_active_source),
            "referenced_removed": keys_to_dicts(referenced_removed),
            "referenced_removed_still_active_in_source": keys_to_dicts(still_active_removed),
            "unreferenced_removed": keys_to_dicts(unreferenced_removed),
            "candidate_attempts": candidate_attempts,
            "source_scan_summary": scan_summary(source_summary),
            "destination_scan_summaries": {str(k): v for k, v in dest_summaries.items()},
        })
        if not referenced_removed and not args.allow_inconclusive:
            fail("No referenced removed candidates found before GC; pass --allow-inconclusive to continue")

        if args.skip_fast_gc:
            gc_response: Dict[str, Any] = {"sql": f"ADMIN GC_REGIONS({source_region_id})", "status_code": "skipped", "response": "skipped"}
        else:
            code, text = sql_query(args.frontend_local_port, f"ADMIN GC_REGIONS({source_region_id})", args.http_timeout)
            gc_response = {"sql": f"ADMIN GC_REGIONS({source_region_id})", "status_code": code, "response": text}
            if code != 200:
                gate_status = "gc_failed"
        write_json(str(out_dir / "sql-gc-source.json"), gc_response)

        referenced_survival = []
        missing = []
        for key in sorted(referenced_removed):
            obj_key = f"{source_data_prefix}{key[0]}.parquet"
            present = s3_head_object(transport, args.bucket, obj_key)
            referenced_survival.append({**key_to_dict(key), "key": obj_key, "present_after_gc": present})
            if not present:
                missing.append(key)
        unreferenced_survival = []
        for key in sorted(unreferenced_removed):
            obj_key = f"{source_data_prefix}{key[0]}.parquet"
            unreferenced_survival.append({**key_to_dict(key), "key": obj_key, "present_after_gc": s3_head_object(transport, args.bucket, obj_key)})
        write_json(str(out_dir / "referenced-removed-survival.json"), referenced_survival)
        write_json(str(out_dir / "unreferenced-removed-survival.json"), unreferenced_survival)
        if missing:
            fail(f"Referenced removed SSTs missing after GC: {keys_to_dicts(set(missing))}")

        after_count = int(sql_query_ok_rows(args.frontend_local_port, f"SELECT COUNT(*) FROM {args.db}.{args.table}", args.http_timeout)[0][0])
        targeted: Dict[str, int] = {}
        for host in ["a_host", "h_host"]:
            targeted[host] = int(sql_query_ok_rows(args.frontend_local_port, f"SELECT COUNT(*) FROM {args.db}.{args.table} WHERE host = '{host}'", args.http_timeout)[0][0])
        write_json(str(out_dir / "sql-read-verification.json"), {"expected_count": expected_count, "actual_count": after_count, "targeted": targeted})
        if after_count != expected_count:
            fail(f"COUNT(*) after GC {after_count} != expected {expected_count}")
        if any(v <= 0 for v in targeted.values()):
            fail(f"Targeted host reads must be nonzero: {targeted}")

        cluster_after = kubectl_json(["get", "greptimedbcluster", args.cluster_name, "-n", args.namespace])
        cluster_phase = cluster_after.get("status", {}).get("clusterPhase", "Unknown")
        write_json(str(out_dir / "cluster-after.json"), cluster_after)
        write_json(str(out_dir / "s3-counts-source-before.json"), s3_before)
        write_json(str(out_dir / "s3-counts-source-after-gc.json"), count_s3_objects(transport, args.bucket, source_prefix))
        if cluster_phase != "Running":
            gate_status = "failed"

        lines = [
            f"EVIDENCE_DIR {args.out}",
            f"PHYSICAL_TABLE {args.db}.{args.physical_table}",
            f"LOGICAL_TABLE {args.db}.{args.table}",
            f"PHYSICAL_TABLE_ID {table_id}",
            f"LOGICAL_TABLE_ID {logical_table_id}",
            f"SOURCE_REGION_ID {source_region_id}",
            f"DEST_REGION_IDS {dest_region_ids}",
            f"REUSED_SOURCE_CHILD_HOST {reused_host}",
            f"EXPECTED_COUNT {expected_count}",
            f"COUNT_AFTER {after_count}",
            f"CANDIDATE_ATTEMPTS {len(candidate_attempts)}",
            f"INCONCLUSIVE {str(not referenced_removed).lower()}",
            f"REFERENCED_REMOVED {len(referenced_removed)}",
            f"UNREFERENCED_REMOVED {len(unreferenced_removed)}",
            f"REFERENCED_REMOVED_MISSING {len(missing)}",
            f"GC_STATUS {gc_response['status_code']}",
            f"CLUSTER_PHASE {cluster_phase}",
            f"GATE_STATUS {gate_status}",
        ]
        write_text(str(out_dir / "concise-summary.txt"), "\n".join(lines) + "\n")
        if gate_status != "passed":
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


if __name__ == "__main__":
    main()
