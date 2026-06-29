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
write_dummy_region_objects.py
Phase 2 lab helper (Python): write/cleanup tiny dummy objects under a live
GreptimeDB region prefix for GC full-listing pressure testing (Test A).

Default transport: MinIO ``mc``.
Optional transport: Python stdlib AWS SigV4 S3 (--transport s3).

No writes or deletes without --execute.
Run with: uv run ...write_dummy_region_objects.py [OPTIONS]

See docs/how-to/how-to-test-gc-huge-file-regions.md for the runbook.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.client
import os
import re
import shutil
import ssl
import subprocess
import sys
import time
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_SAFE_SUBSTRING = "gt-gc-hf-"
DEFAULT_MAX_SIZE_BYTES = 1024
DEFAULT_CONCURRENCY = 4
DEFAULT_S3_REGION = "us-west-2"

# UUID v4 regex for path-shape validation (case-insensitive)
UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)

# Legal path shapes for cleanup manifest entries:
#   <prefix>/<uuid>.parquet
#   <prefix>/index/<uuid>.puffin
#   <prefix>/index/<uuid>.<int>.puffin
PARQUET_PATH_RE = re.compile(rf"/({UUID_RE.pattern})\.parquet$")
INDEX_PATH_RE = re.compile(rf"/index/({UUID_RE.pattern})\.puffin$")
INDEX_VERSIONED_PATH_RE = re.compile(rf"/index/({UUID_RE.pattern})\.(\d+)\.puffin$")

# S3 XML namespace for ListObjectsV2 response parsing
_S3_XML_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _s3_xml_tag(name: str) -> str:
    return f"{{{_S3_XML_NS}}}{name}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def check_mc() -> None:
    if shutil.which("mc") is None:
        fail("mc not found in PATH")


def count_objects(region_prefix: str) -> int:
    """Count objects under a prefix via ``mc ls --recursive`` without buffering output."""
    proc = subprocess.Popen(
        ["mc", "ls", "--recursive", region_prefix],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    count = 0
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            if line.strip():
                count += 1
        rc = proc.wait()
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait()
    return count if rc == 0 else -1


def check_leading_dash(value: str, name: str) -> None:
    if value.startswith("-"):
        fail(f"{name} must not begin with '-': {value}")


def iter_manifest_targets(manifest_path: str) -> Iterable[str]:
    """Yield non-empty target paths exactly as validated.

    Do not call ``strip()`` here: validation rejects leading/trailing
    whitespace, so execution must use the same line normalization and not turn a
    validated-safe line like ``" -foo"`` into a dash-leading target.
    """
    with open(manifest_path, "r") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if line:
                yield line


def run_bounded(
    items,
    worker,
    concurrency: int,
    sleep_ms: int,
    total: int,
    progress_label: str,
) -> tuple[int, list[str]]:
    """Run subprocess workers with bounded in-flight futures.

    This avoids submitting millions of futures at once for large lab runs.
    The worker returns ``None`` on success or an error string on failure.
    """
    failures: list[str] = []
    completed = 0
    iterator = iter(items)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        pending = set()

        def submit_one() -> bool:
            try:
                item = next(iterator)
            except StopIteration:
                return False
            pending.add(executor.submit(worker, item))
            return True

        for _ in range(concurrency):
            if not submit_one():
                break

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                err = future.result()
                if err:
                    failures.append(err)
                completed += 1
                if sleep_ms > 0:
                    time.sleep(sleep_ms / 1000.0)
                if completed % 1000 == 0:
                    print(f"  progress: {completed} / {total} {progress_label}")
                submit_one()

    return completed, failures


# ---------------------------------------------------------------------------
# S3 Transport (pure Python stdlib, AWS SigV4)
# ---------------------------------------------------------------------------
class S3Transport:
    """Minimal S3 client using Python stdlib with AWS SigV4 signing.

    Path-style URLs: {endpoint}/{bucket}/{key}
    Compatible with MinIO and AWS S3.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 region: str = DEFAULT_S3_REGION):
        # Normalise endpoint: ensure scheme present, no trailing slash
        if "://" not in endpoint:
            endpoint = f"http://{endpoint}"
        parsed = urllib.parse.urlparse(endpoint.rstrip("/"))
        self.scheme = parsed.scheme or "http"
        self.host = parsed.hostname or "127.0.0.1"
        self.port = parsed.port or (443 if self.scheme == "https" else 80)
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.service = "s3"

    # -- SigV4 helpers -------------------------------------------------------

    @staticmethod
    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    @staticmethod
    def _sha256_hex(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _signing_key(self, date_stamp: str) -> bytes:
        k_date = self._sign(f"AWS4{self.secret_key}".encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, self.region)
        k_service = self._sign(k_region, self.service)
        return self._sign(k_service, "aws4_request")

    @staticmethod
    def _sorted_query_string(params: List[Tuple[str, str]]) -> str:
        """Build a SigV4-safe canonical query string.

        Parameters are sorted by name, then encoded with ``urllib.parse.quote``
        (produces ``%20`` for spaces, not ``+``).  This is consistent with
        AWS SigV4 canonical-query-string requirements.
        """
        sorted_params = sorted(params, key=lambda x: x[0])
        return urllib.parse.urlencode(sorted_params, quote_via=urllib.parse.quote)

    def _sign_headers(self, method: str, bucket: str, key: str,
                      body: bytes = b"", query_string: str = "") -> dict:
        """Build signed request headers for a path-style S3 request."""
        t = datetime.now(timezone.utc)
        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = t.strftime("%Y%m%d")

        # Canonical URI: path-style = /{bucket}/{url-encoded-key}
        encoded_key = urllib.parse.quote(key, safe="/~") if key else ""
        if encoded_key:
            canonical_uri = f"/{bucket}/{encoded_key}"
        else:
            canonical_uri = f"/{bucket}/"

        payload_hash = self._sha256_hex(body)

        host_header = self.host
        if self.port not in (80, 443):
            host_header = f"{self.host}:{self.port}"

        headers = {
            "Host": host_header,
            "X-Amz-Content-SHA256": payload_hash,
            "X-Amz-Date": amz_date,
        }
        if body:
            headers["Content-Type"] = "application/octet-stream"
            headers["Content-Length"] = str(len(body))

        # Build canonical request
        signed_headers = ";".join(sorted(h.lower() for h in headers))
        canonical_headers = "\n".join(
            f"{h.lower()}:{v}"
            for h, v in sorted(headers.items(), key=lambda x: x[0].lower())
        )

        canonical_request = (
            f"{method}\n{canonical_uri}\n{query_string}\n"
            f"{canonical_headers}\n\n{signed_headers}\n{payload_hash}"
        )

        credential_scope = f"{date_stamp}/{self.region}/{self.service}/aws4_request"
        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
            f"{self._sha256_hex(canonical_request.encode('utf-8'))}"
        )

        signing_key = self._signing_key(date_stamp)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        headers["Authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        return headers

    def _request(self, method: str, bucket: str, key: str = "",
                 body: Optional[bytes] = None,
                 query_string: str = "") -> Tuple[int, bytes]:
        """Low-level signed HTTP request.  Returns (status, body)."""
        if body is None:
            body = b""
        headers = self._sign_headers(method, bucket, key, body, query_string)

        encoded_key = urllib.parse.quote(key, safe="/~") if key else ""
        path = f"/{bucket}/{encoded_key}" if encoded_key else f"/{bucket}/"
        if query_string:
            path += "?" + query_string

        if self.scheme == "https":
            ctx = ssl.create_default_context()
            conn = http.client.HTTPSConnection(self.host, self.port, context=ctx)
        else:
            conn = http.client.HTTPConnection(self.host, self.port)

        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            if resp.status >= 400:
                snippet = data[:500].decode(errors="replace")
                raise RuntimeError(
                    f"S3 {method} {bucket}/{key} failed: "
                    f"HTTP {resp.status}: {snippet}"
                )
            return resp.status, data
        finally:
            conn.close()

    # -- Public S3 operations ------------------------------------------------

    def list_objects_v2(self, bucket: str, prefix: str, max_keys: int = 1000):
        """Generator yielding ``(key, size)`` tuples.  Handles pagination.

        Query parameters are explicitly sorted for canonical query string
        correctness (AWS SigV4 requires sorted, percent-encoded params).
        """
        continuation_token: Optional[str] = None
        while True:
            params: List[Tuple[str, str]] = [
                ("list-type", "2"),
                ("max-keys", str(max_keys)),
                ("prefix", prefix),
            ]
            if continuation_token:
                params.append(("continuation-token", continuation_token))

            query = self._sorted_query_string(params)
            _, data = self._request("GET", bucket, "", query_string=query)

            root = ET.fromstring(data)
            for contents in root.findall(_s3_xml_tag("Contents")):
                key_el = contents.find(_s3_xml_tag("Key"))
                size_el = contents.find(_s3_xml_tag("Size"))
                if key_el is not None:
                    yield (
                        key_el.text or "",
                        int(size_el.text or "0") if size_el is not None else 0,
                    )

            is_truncated = root.find(_s3_xml_tag("IsTruncated"))
            next_token = root.find(_s3_xml_tag("NextContinuationToken"))
            if is_truncated is None or is_truncated.text != "true":
                break
            if next_token is None or not next_token.text:
                break
            continuation_token = next_token.text

    def count_objects(self, bucket: str, prefix: str) -> int:
        """Count objects under *prefix* via paginated ListObjectsV2."""
        count = 0
        for _ in self.list_objects_v2(bucket, prefix):
            count += 1
        return count

    def find_region_object(self, bucket: str, prefix: str) -> bool:
        """Return True if any .parquet or .puffin exists under *prefix*."""
        match_re = re.compile(r"\.(parquet|puffin)$")
        for key, _ in self.list_objects_v2(bucket, prefix):
            if match_re.search(key):
                return True
        return False

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        """PUT an object."""
        self._request("PUT", bucket, key, body=body)

    def delete_object(self, bucket: str, key: str) -> None:
        """DELETE an object."""
        self._request("DELETE", bucket, key)


# ---------------------------------------------------------------------------
# S3 helpers: prefix / key extraction
# ---------------------------------------------------------------------------
def parse_s3_region_prefix(prefix: str, has_alias: bool = False,
                           expected_bucket: str = "") -> Tuple[str, str, str]:
    """Parse a user-provided region prefix for S3 transport.

    Returns ``(bucket, key_prefix, display_path)``.

    **Strict disambiguation** — no heuristic guessing:

    * ``s3://bucket/key/`` — parsed directly.
      If *expected_bucket* is set, it must match.

    * With ``--s3-prefix-has-alias`` (``has_alias=True``):
      ``alias/bucket/key/`` — first segment = alias (ignored),
      second = bucket, rest = key_prefix.

    * Otherwise (bare ``bucket/key/``):
      *expected_bucket* is **required** and must match the first segment.

    *display_path* is the normalised path used in manifest entries and
    guard checks (alias stripped, ``s3://`` stripped).
    """
    p = prefix.rstrip("/") + "/"

    # --- s3:// prefix (unambiguous) ----------------------------------------
    if p.startswith("s3://"):
        p = p[5:]
        parts = [x for x in p.split("/") if x]
        if not parts:
            fail(f"S3 prefix is empty after s3://: {prefix!r}")
        bucket = parts[0]
        key_prefix = "/".join(parts[1:]) + "/" if len(parts) > 1 else ""
        display = p
        if expected_bucket and bucket != expected_bucket:
            fail(
                f"S3 prefix bucket '{bucket}' does not match "
                f"--bucket '{expected_bucket}'"
            )
        return bucket, key_prefix, display

    # --- Non-s3:// prefix --------------------------------------------------
    parts = [x for x in p.split("/") if x]

    if has_alias:
        # Explicit mc-style: alias/bucket/key/...
        if len(parts) < 3:
            fail(
                "--s3-prefix-has-alias requires at least 3 parts "
                f"(alias/bucket/key/...); got: {prefix!r}"
            )
        bucket = parts[1]
        key_prefix = "/".join(parts[2:]) + "/"
        display = "/".join(parts[1:]) + "/"  # strip alias
        if expected_bucket and bucket != expected_bucket:
            fail(
                f"Prefix bucket '{bucket}' does not match "
                f"--bucket '{expected_bucket}'"
            )
        return bucket, key_prefix, display

    # No s3://, no alias: first segment MUST be bucket
    if not expected_bucket:
        fail(
            "S3 direct prefix without s3:// requires --bucket to disambiguate.\n"
            "  Use one of:\n"
            "  1) --region-prefix s3://bucket/key/\n"
            "  2) --region-prefix alias/bucket/key/ --s3-prefix-has-alias\n"
            "  3) --region-prefix bucket/key/ --bucket bucket"
        )

    if not parts:
        fail(f"Cannot parse S3 region prefix: {prefix!r}")

    bucket = parts[0]
    if bucket != expected_bucket:
        fail(
            f"Prefix first segment '{bucket}' must match "
            f"--bucket '{expected_bucket}'"
        )
    key_prefix = "/".join(parts[1:]) + "/" if len(parts) > 1 else ""
    return bucket, key_prefix, p


def s3_extract_key(target: str, bucket: str) -> str:
    """Extract S3 object key from a manifest target.  **Fail-closed**.

    Accepted forms (bucket must match *bucket*):

    * ``s3://<bucket>/<rest>``
    * ``<alias>/<bucket>/<rest>``  — alias tolerated (e.g. from mc-generated manifest)
    * ``<bucket>/<rest>``

    Any mismatch or unrecognised form raises ``ValueError``.
    """
    t = target
    s3_prefix = False
    if t.startswith("s3://"):
        s3_prefix = True
        t = t[5:]

    if s3_prefix:
        expected = bucket + "/"
        if not t.startswith(expected):
            raise ValueError(
                f"S3 manifest entry bucket mismatch: expected 's3://{bucket}/...', "
                f"got: {target!r}"
            )
        return t[len(expected):]

    expected = bucket + "/"
    if t.startswith(expected):
        return t[len(expected):]

    # Explicit alias form only: <alias>/<bucket>/<rest>.  Do not search for the
    # bucket name at arbitrary later positions because the key itself may contain
    # a path segment equal to the bucket name.
    parts = t.split("/", 2)
    if len(parts) == 3 and parts[1] == bucket and parts[2]:
        return parts[2]

    raise ValueError(
        f"Manifest entry bucket mismatch: expected '{bucket}/...' or "
        f"'<alias>/{bucket}/...', got: {target!r}"
    )


def validate_s3_cleanup_bucket(manifest_path: str, bucket: str) -> int:
    """Pre-validate every manifest entry for correct bucket.  Returns count."""
    count = 0
    errors: List[str] = []
    for line in iter_manifest_targets(manifest_path):
        count += 1
        try:
            s3_extract_key(line, bucket)
        except ValueError as e:
            errors.append(str(e))
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        fail(
            f"{len(errors)} manifest entries have bucket mismatch "
            f"(expected bucket: '{bucket}')"
        )
    return count


def _s3_transport_from_args(args: argparse.Namespace) -> S3Transport:
    """Create (or return cached) S3Transport from CLI args."""
    if not hasattr(args, "_s3t_cache"):
        args._s3t_cache = S3Transport(
            endpoint=args.s3_endpoint,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key,
            region=args.s3_region,
        )
    return args._s3t_cache


# ---------------------------------------------------------------------------
# Prefix computation
# ---------------------------------------------------------------------------
def compute_prefix(args: argparse.Namespace) -> str:
    """Compute the region display prefix from direct or computed args."""
    if args.region_prefix:
        return args.region_prefix.rstrip("/") + "/"

    # Computed prefix
    table_dir = "data"
    if args.storage_path:
        table_dir = f"{table_dir}/{args.storage_path}"
    table_dir = f"{table_dir}/{args.table_id}"

    region_name = f"{args.table_id}_{int(args.region_sequence):010d}"
    base_region_dir = f"{table_dir}/{region_name}"

    if args.transport == "s3":
        # S3 transport: no mc_alias in display path
        prefix = f"{args.bucket}"
    else:
        prefix = ""
        if args.mc_alias:
            prefix = f"{args.mc_alias}/"
        prefix = f"{prefix}{args.bucket}"

    mid = ""
    if args.root_prefix:
        mid = f"{args.root_prefix}/"

    if args.path_type == "bare":
        return f"{prefix}/{mid}{base_region_dir}/"
    else:  # data
        return f"{prefix}/{mid}{base_region_dir}/data/"


# ---------------------------------------------------------------------------
# UUID path generation
# ---------------------------------------------------------------------------
def object_path(region_prefix: str, uid: str, args: argparse.Namespace) -> str:
    """Generate target object path for a given UUID."""
    if args.object_kind == "parquet":
        return f"{region_prefix}{uid}.parquet"
    elif args.object_kind == "index":
        return f"{region_prefix}index/{uid}.puffin"
    else:  # index-versioned
        ver = args.index_version if args.index_version else 1
        return f"{region_prefix}index/{uid}.{ver}.puffin"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _validate_safe_substring(safe_substring: str, allow_unsafe: bool) -> None:
    """Reject empty --safe-substring unless --allow-unsafe-prefix."""
    if not safe_substring and not allow_unsafe:
        fail(
            "--safe-substring must not be empty. "
            "Use --allow-unsafe-prefix to bypass."
        )


def validate_write_mode(args: argparse.Namespace) -> None:
    """Validate write-mode arguments (called after parsing)."""
    if args.count <= 0:
        fail("--count must be a positive integer")

    if args.object_kind not in ("parquet", "index", "index-versioned"):
        fail("--object-kind must be parquet, index, or index-versioned")

    if args.object_kind == "index-versioned" and args.index_version is None:
        args.index_version = 1

    if args.index_version is not None and args.index_version < 0:
        fail("--index-version must be a non-negative integer")

    if args.size_bytes < 0:
        fail("--size-bytes must be a non-negative integer")

    if args.max_size_bytes < 1:
        fail("--max-size-bytes must be a positive integer")

    if not args.allow_large_objects and args.size_bytes > args.max_size_bytes:
        fail(
            f"--size-bytes ({args.size_bytes}) exceeds max ({args.max_size_bytes}). "
            f"Use --allow-large-objects or --max-size-bytes to override."
        )

    if args.concurrency < 1:
        fail("--concurrency must be a positive integer")

    if args.sleep_ms < 0:
        fail("--sleep-ms must be a non-negative integer")

    # Safe substring must not be empty
    _validate_safe_substring(args.safe_substring, args.allow_unsafe_prefix)

    # Manifest overwrite safety
    if args.manifest and os.path.exists(args.manifest) and not args.overwrite_manifest:
        fail(
            f"manifest exists: {args.manifest}. "
            f"Use --overwrite-manifest to overwrite."
        )

    # Prefix source validation
    if args.region_prefix:
        check_leading_dash(args.region_prefix, "--region-prefix")
    elif args.bucket and args.table_id is not None and args.region_sequence is not None:
        # Computed prefix (mc alias optional for S3 transport)
        if args.transport == "mc" and not args.mc_alias:
            fail("--mc-alias is required for computed prefix in mc transport mode")
        if args.path_type not in ("bare", "data"):
            fail(f"--path-type must be 'bare' or 'data' (got: '{args.path_type}')")
        if args.region_sequence is not None and args.region_sequence < 0:
            fail("--region-sequence must be a non-negative integer")
        if not re.match(r"^\d+$", str(args.table_id)):
            fail(f"--table-id must be numeric (got: '{args.table_id}')")
        if str(args.table_id) != str(args.table_id).strip() or str(args.table_id) == "":
            fail("--table-id must not be empty")
        if not args.storage_path and not args.allow_empty_storage_path:
            fail(
                "--storage-path is required for computed prefix. "
                "Use --allow-empty-storage-path if the storage path is genuinely empty."
            )
    else:
        # mc transport requires mc-alias; s3 transport requires bucket
        if args.transport == "mc":
            fail(
                "must provide --region-prefix OR "
                "(--mc-alias, --bucket, --table-id, --region-sequence)"
            )
        else:
            fail(
                "must provide --region-prefix OR "
                "(--bucket, --table-id, --region-sequence)"
            )

    # Write guard: execute requires verification
    if not args.dry_run:
        if not args.i_verified_prefix and not args.verify_existing_region_object:
            fail(
                "--execute requires --i-verified-prefix or "
                "--verify-existing-region-object"
            )

    # S3 transport: require endpoint + credentials when executing
    if args.transport == "s3" and not args.dry_run:
        if not args.s3_endpoint:
            fail("--s3-endpoint is required for --transport s3 (--execute)")
        if not args.s3_access_key:
            fail("--s3-access-key is required for --transport s3 (--execute)")
        if not args.s3_secret_key:
            fail("--s3-secret-key is required for --transport s3 (--execute)")


def validate_cleanup_mode(args: argparse.Namespace) -> None:
    """Validate cleanup-mode arguments."""
    if not args.manifest:
        fail("--cleanup requires --manifest PATH")
    if not os.path.isfile(args.manifest):
        fail(f"manifest not found: {args.manifest}")
    if args.concurrency < 1:
        fail("--concurrency must be a positive integer")
    if args.sleep_ms < 0:
        fail("--sleep-ms must be a non-negative integer")
    # Safe substring must not be empty
    _validate_safe_substring(args.safe_substring, args.allow_unsafe_prefix)
    # Cleanup execute guard (before any network call)
    if not args.dry_run:
        if not args.i_verified_prefix and not args.i_verified_cleanup_manifest:
            fail(
                "--execute (cleanup) requires --i-verified-prefix or "
                "--i-verified-cleanup-manifest.  "
                "Review the manifest carefully before proceeding."
            )
    # S3 transport: require endpoint + credentials when executing
    if args.transport == "s3" and not args.dry_run:
        if not args.s3_endpoint:
            fail("--s3-endpoint is required for --transport s3 (--execute)")
        if not args.s3_access_key:
            fail("--s3-access-key is required for --transport s3 (--execute)")
        if not args.s3_secret_key:
            fail("--s3-secret-key is required for --transport s3 (--execute)")
        if not args.bucket:
            fail("--bucket is required for --transport s3 in cleanup mode")


def validate_cleanup_manifest(
    manifest_path: str,
    safe_substring: str,
    allow_unsafe: bool,
    allow_multi: bool,
) -> int:
    """Validate cleanup manifest entries. Returns line count or exits on error."""
    errors: List[str] = []
    prefixes: Set[str] = set()
    lines: List[str] = []

    with open(manifest_path, "r") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if not line:
                continue
            if line != line.strip():
                errors.append(
                    f"line {len(lines) + 1}: entry has leading/trailing whitespace: {line!r}"
                )
                lines.append(line)
                continue
            lines.append(line)

    if not lines:
        fail("cleanup manifest is empty")

    for idx, line in enumerate(lines, start=1):
        # Reject leading dash
        if line.startswith("-"):
            errors.append(f"line {idx}: entry begins with '-' (unsafe): {line}")
            continue

        # Validate path shape
        if PARQUET_PATH_RE.search(line):
            pass
        elif INDEX_VERSIONED_PATH_RE.search(line):
            pass
        elif INDEX_PATH_RE.search(line):
            pass
        else:
            errors.append(f"line {idx}: invalid path shape: {line}")
            continue

        # Extract prefix
        if "/index/" in line:
            prefix = line.rsplit("/index/", 1)[0]
        else:
            prefix = line.rsplit("/", 1)[0]
        prefixes.add(prefix)

        # Safe substring check
        if not allow_unsafe and safe_substring not in line:
            errors.append(
                f"line {idx}: entry does not contain '{safe_substring}': {line}"
            )

    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        if not allow_unsafe:
            print(
                "Use --allow-unsafe-prefix to bypass safe-substring checks.",
                file=sys.stderr,
            )
        sys.exit(1)

    # Multi-prefix check
    if len(prefixes) > 1 and not allow_multi:
        print(
            f"ERROR: manifest contains {len(prefixes)} distinct region prefixes.",
            file=sys.stderr,
        )
        print("  Use --allow-multiple-prefixes to override.", file=sys.stderr)
        for p in sorted(prefixes):
            print(f"    {p}", file=sys.stderr)
        sys.exit(1)

    print(f"  manifest validation: {len(lines)} lines, {len(prefixes)} distinct prefix(es) — OK")
    return len(lines)


# ---------------------------------------------------------------------------
# Guard: safe substring check
# ---------------------------------------------------------------------------
def check_prefix_guard(region_prefix: str, safe_substring: str, allow_unsafe: bool) -> bool:
    if region_prefix.startswith("-"):
        print("  guard: FAILED — prefix begins with '-' and could be interpreted as options")
        print(f"  prefix: {region_prefix}")
        return False
    if allow_unsafe:
        print("  guard: unsafe prefix allowed (--allow-unsafe-prefix)")
        return True
    if safe_substring in region_prefix:
        print(f"  guard: prefix contains safe substring '{safe_substring}' — OK")
        return True
    print(f"  guard: FAILED — prefix does not contain '{safe_substring}'")
    print(f"  prefix: {region_prefix}")
    print("  Use --allow-unsafe-prefix to override.")
    return False


# ---------------------------------------------------------------------------
# Verify existing region object (streamed, stops at first match)
# ---------------------------------------------------------------------------
def verify_existing_region_object(args: argparse.Namespace,
                                  region_prefix: str,
                                  s3_bucket: Optional[str] = None,
                                  s3_key_prefix: Optional[str] = None) -> bool:
    """Stream list and stop at first .parquet/.puffin match.

    For S3 transport the O(n) ListObjectsV2 may be slower for huge prefixes;
    prefer ``--i-verified-prefix`` when practical.
    """
    if not args.verify_existing_region_object:
        return False

    print("=== verifying existing region objects ===")

    if args.dry_run:
        transport_name = args.transport.upper()
        print(f"  dry-run: would stream {transport_name} ls until first .parquet/.puffin")
        return True

    if args.transport == "s3":
        t = _s3_transport_from_args(args)
        assert s3_bucket and s3_key_prefix is not None
        found = t.find_region_object(s3_bucket, s3_key_prefix)
        if found:
            print("  found existing .parquet or .puffin under prefix — OK")
            return True
        fail("no .parquet or .puffin found under prefix")
    else:
        check_mc()
        match_re = re.compile(r"\.(parquet|puffin)$")
        proc = subprocess.Popen(
            ["mc", "ls", "--recursive", region_prefix],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                if match_re.search(line):
                    print("  found existing .parquet or .puffin under prefix — OK")
                    proc.terminate()
                    return True
            proc.wait()
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait()

    fail("no .parquet or .puffin found under prefix")
    return False  # unreachable


# ---------------------------------------------------------------------------
# Summary output
# ---------------------------------------------------------------------------
def print_summary(args: argparse.Namespace, region_prefix: str,
                  s3_info: Optional[Tuple[str, str]] = None) -> None:
    """Print mode/settings/preview summary."""
    executing = "YES" if not args.dry_run else "NO (dry-run)"
    mode = "cleanup" if args.cleanup else "write"
    print("=== SUMMARY ===")
    print(f"  mode:        {mode}")
    print(f"  transport:   {args.transport}")
    print(f"  executing:   {executing}")
    print(f"  region-prefix: {region_prefix}")
    print(f"  object-kind:   {args.object_kind}")
    if s3_info:
        s3_bucket, s3_key_prefix = s3_info
        print(f"  s3-bucket:     {s3_bucket}")
        print(f"  s3-key-prefix: {s3_key_prefix}")

    if not args.cleanup:
        print(f"  count:         {args.count}")
        print(f"  size-bytes:    {args.size_bytes}")
        print(f"  max-size-bytes: {args.max_size_bytes}")
        print(f"  concurrency:   {args.concurrency}")
        print(f"  sleep-ms:      {args.sleep_ms}")
        if args.manifest:
            print(f"  manifest:      {args.manifest}")
        if args.skip_count_verification:
            print("  count-verify:  SKIPPED (--skip-count-verification)")
        print()
        print("=== sample targets (first 5) ===")
        for i in range(min(5, args.count)):
            uid = str(uuid.uuid4())
            print(f"  {object_path(region_prefix, uid, args)}")
        print(f"  ... ({args.count} total objects)")
    else:
        print(f"  manifest:      {args.manifest}")
        with open(args.manifest, "r") as fh:
            line_count = sum(1 for _ in fh)
        print(f"  entries:       {line_count}")

    print()
    print("=== guards ===")
    print(f"  i-verified-prefix:          {args.i_verified_prefix}")
    print(f"  i-verified-cleanup-manifest: {getattr(args, 'i_verified_cleanup_manifest', False)}")
    print(f"  verify-existing:            {args.verify_existing_region_object}")
    print(f"  allow-unsafe:               {args.allow_unsafe_prefix}")
    print(f"  allow-multi-prefix:         {args.allow_multiple_prefixes}")
    print(f"  safe-substring:             '{args.safe_substring}'")


# ---------------------------------------------------------------------------
# Write mode
# ---------------------------------------------------------------------------
def do_write(args: argparse.Namespace, region_prefix: str,
             s3_bucket: Optional[str] = None,
             s3_key_prefix: Optional[str] = None) -> None:
    """Generate manifest, optionally write objects, verify count."""
    # Generate manifest
    print("=== generating object names ===")

    manifest_path: str
    if args.manifest:
        manifest_path = args.manifest
    else:
        import tempfile
        fd, manifest_path = tempfile.mkstemp(prefix="gc_dummy_manifest.", suffix=".txt")
        os.close(fd)
        print(f"  (no --manifest specified, using tmp: {manifest_path})")

    with open(manifest_path, "w") as fh:
        for _ in range(args.count):
            uid = str(uuid.uuid4())
            target = object_path(region_prefix, uid, args)
            check_leading_dash(target, "generated target path")
            fh.write(target + "\n")

    print(f"  manifest written: {manifest_path} ({args.count} entries)")

    if args.dry_run:
        transport_kind = "mc pipe" if args.transport == "mc" else "S3 PUT"
        print(f"  dry-run: would write {args.count} objects via {transport_kind}")
        return

    # Execute writes
    print("=== writing objects to object store ===")

    # Content payload
    content = "x" * args.size_bytes if args.size_bytes > 0 else ""

    # Before-count
    before_count = -1

    if args.transport == "s3":
        assert s3_bucket and s3_key_prefix is not None
        t = _s3_transport_from_args(args)

        if not args.skip_count_verification:
            print("  capturing before-count...")
            before_count = t.count_objects(s3_bucket, s3_key_prefix)
            print(f"  before-count: {before_count}")

        content_bytes = content.encode("utf-8")

        def upload(target: str) -> Optional[str]:
            try:
                key = s3_extract_key(target, s3_bucket)
                t.put_object(s3_bucket, key, content_bytes)
                return None
            except Exception as e:
                return f"upload failed: {target}: {e}"

    else:
        check_mc()

        if not args.skip_count_verification:
            print("  capturing before-count...")
            before_count = count_objects(region_prefix)
            print(f"  before-count: {before_count}")

        def upload(target: str) -> Optional[str]:
            try:
                subprocess.run(
                    ["mc", "pipe", target],
                    input=content, text=True, capture_output=True, check=True,
                )
                return None
            except subprocess.CalledProcessError as e:
                return f"upload failed: {target}: {e}"

    written, failures = run_bounded(
        iter_manifest_targets(manifest_path),
        upload,
        args.concurrency,
        args.sleep_ms,
        args.count,
        "uploads",
    )

    if failures:
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        fail(f"{len(failures)} / {args.count} uploads failed")

    print(f"=== write complete: {args.count} objects (0 failures) ===")

    # Verify count
    if not args.skip_count_verification:
        print("=== verifying object count ===")

        if args.transport == "s3":
            assert s3_bucket and s3_key_prefix is not None
            after_count = _s3_transport_from_args(args).count_objects(s3_bucket, s3_key_prefix)
        else:
            after_count = count_objects(region_prefix)

        print(f"  after-count: {after_count}")

        if before_count >= 0 and after_count >= 0:
            delta = after_count - before_count
            if delta != args.count:
                fail(
                    f"object count delta ({delta}) != expected ({args.count}). "
                    f"before: {before_count}, after: {after_count}"
                )
            print(f"  count delta matches expected (+{args.count}) — OK")
        else:
            fail(
                f"cannot compute count delta (before={before_count}, after={after_count}). "
                f"Use --skip-count-verification to bypass."
            )
    else:
        print("  count verification skipped (--skip-count-verification)")


# ---------------------------------------------------------------------------
# Cleanup mode
# ---------------------------------------------------------------------------
def do_cleanup(args: argparse.Namespace,
               s3_bucket: Optional[str] = None) -> None:
    """Delete objects listed in a manifest."""
    print("=== cleanup mode ===")
    print(f"  manifest: {args.manifest}")

    # Read targets for count/sample only. The execute path streams from the manifest.
    targets = list(iter_manifest_targets(args.manifest))

    print(f"  entries:  {len(targets)}")

    if not targets:
        if args.dry_run:
            print("  WARNING: cleanup manifest is empty — nothing to do")
            return
        fail("cleanup manifest is empty; refusing to execute")

    if args.dry_run:
        transport_kind = "mc rm" if args.transport == "mc" else "S3 DELETE"
        print(f"  dry-run: would delete {len(targets)} objects via {transport_kind}")
        print()
        print("  sample targets (first 5):")
        for t in targets[:5]:
            print(f"    {t}")
        return

    print("=== deleting objects ===")

    if args.transport == "s3":
        assert s3_bucket is not None
        t = _s3_transport_from_args(args)

        def remove(target: str) -> Optional[str]:
            try:
                key = s3_extract_key(target, s3_bucket)
                t.delete_object(s3_bucket, key)
                return None
            except Exception as e:
                return f"delete failed: {target}: {e}"
    else:
        check_mc()

        def remove(target: str) -> Optional[str]:
            try:
                subprocess.run(["mc", "rm", target], capture_output=True, check=True)
                return None
            except subprocess.CalledProcessError as e:
                return f"delete failed: {target}: {e}"

    deleted, failures = run_bounded(
        iter_manifest_targets(args.manifest),
        remove,
        args.concurrency,
        args.sleep_ms,
        len(targets),
        "deletes",
    )

    if failures:
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        fail(f"{len(failures)} deletes failed")

    print(f"=== cleanup complete: {deleted} attempted, 0 errors ===")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Write/cleanup tiny dummy objects under a GreptimeDB region prefix.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run direct prefix (mc)
  uv run ...write_dummy_region_objects.py \\
    --region-prefix myminio/gtbucket/gt-gc-hf-test/data/1024/1024_0000000000/ \\
    --count 1000 --manifest /tmp/gc_manifest.txt

  # Computed prefix dry-run (mc)
  uv run ...write_dummy_region_objects.py \\
    --mc-alias myminio --bucket gtbucket --root-prefix greptimedb-data \\
    --storage-path greptimedb --table-id 1024 --region-sequence 0 \\
    --path-type bare --count 100

  # S3 transport direct prefix dry-run (s3:// form)
  uv run ...write_dummy_region_objects.py \\
    --transport s3 --s3-endpoint http://127.0.0.1:19000 \\
    --s3-access-key <access-key> --s3-secret-key <secret-key> \\
    --region-prefix s3://gc-stress-bucket/gc-hf-lab/data/greptime/public/1024/1024_0000000000/ \\
    --count 1000 --manifest /tmp/gc_manifest.txt

  # S3 transport direct prefix dry-run (mc-style with --s3-prefix-has-alias)
  uv run ...write_dummy_region_objects.py \\
    --transport s3 --s3-endpoint http://127.0.0.1:19000 \\
    --s3-access-key <access-key> --s3-secret-key <secret-key> \\
    --region-prefix myminio/gc-stress-bucket/gc-hf-lab/.../ \\
    --s3-prefix-has-alias \\
    --count 100 --manifest /tmp/gc_manifest.txt

  # S3 transport direct prefix dry-run (bare, requires --bucket)
  uv run ...write_dummy_region_objects.py \\
    --transport s3 --s3-endpoint http://127.0.0.1:19000 \\
    --s3-access-key <access-key> --s3-secret-key <secret-key> \\
    --region-prefix gc-stress-bucket/gc-hf-lab/.../ \\
    --bucket gc-stress-bucket \\
    --count 100 --manifest /tmp/gc_manifest.txt

  # Write (after verifying existing region objects)
  uv run ...write_dummy_region_objects.py \\
    --region-prefix ... --count 1000 --manifest /tmp/gc_manifest.txt \\
    --overwrite-manifest --verify-existing-region-object --execute

  # Cleanup dry-run
  uv run ...write_dummy_region_objects.py \\
    --cleanup --manifest /tmp/gc_manifest.txt

  # Cleanup execute
  uv run ...write_dummy_region_objects.py \\
    --cleanup --manifest /tmp/gc_manifest.txt \\
    --i-verified-cleanup-manifest --execute
""",
    )

    # Mode
    p.add_argument("--execute", action="store_true", dest="dry_run_false",
                   help="Enable actual writes/deletes (default: dry-run).")
    p.add_argument("--cleanup", action="store_true",
                   help="Cleanup mode: delete objects listed in a manifest.")

    # Transport
    g = p.add_argument_group("transport")
    g.add_argument("--transport", choices=["mc", "s3"], default="mc",
                   help="Object-store transport (default: mc)")
    g.add_argument("--s3-endpoint",
                   help="S3/MinIO endpoint URL, e.g. http://127.0.0.1:19000")
    g.add_argument("--s3-access-key", help="S3 access key")
    g.add_argument("--s3-secret-key", help="S3 secret key")
    g.add_argument("--s3-region", default=DEFAULT_S3_REGION,
                   help=f"S3 region (default: {DEFAULT_S3_REGION})")

    # Prefix
    g = p.add_argument_group("prefix (choose one)")
    g.add_argument("--region-prefix",
                   help="Direct prefix. "
                        "mc mode: <alias>/<bucket>/<path/>. "
                        "s3 mode: s3://<bucket>/<path/> or <bucket>/<path/> (requires --bucket).")
    g.add_argument("--s3-prefix-has-alias", action="store_true",
                   help="[S3 only] --region-prefix is mc-style alias/bucket/key/ "
                        "(alias will be stripped)")
    g.add_argument("--mc-alias", help="mc alias name (for computed prefix)")
    g.add_argument("--bucket", help="Bucket name (for computed prefix or S3 transport)")
    g.add_argument("--root-prefix", help="Root prefix inside bucket")
    g.add_argument("--storage-path", help="Storage path component (required in computed mode)")
    g.add_argument("--allow-empty-storage-path", action="store_true",
                   help="Bypass storage-path requirement in computed mode")
    g.add_argument("--table-id", help="Numeric table id (required in computed mode)")
    g.add_argument("--region-sequence", type=int,
                   help="Region sequence number (zero-padded to 010)")
    g.add_argument("--path-type", choices=["bare", "data"],
                   help="Mito/Mito2 -> bare; metric-engine -> data")

    # Safety
    g = p.add_argument_group("safety (required for --execute)")
    g.add_argument("--i-verified-prefix", action="store_true",
                   help="Assert you have independently verified the exact region prefix")
    g.add_argument("--i-verified-cleanup-manifest", action="store_true",
                   help="Assert you have independently reviewed the cleanup manifest. "
                        "Required for --execute in cleanup mode.")
    g.add_argument("--verify-existing-region-object", action="store_true",
                   help="Stream list until first .parquet/.puffin to confirm a live region")
    g.add_argument("--allow-unsafe-prefix", action="store_true",
                   help="Bypass safe-substring guard (including empty substring)")
    g.add_argument("--allow-multiple-prefixes", action="store_true",
                   help="Allow a cleanup manifest to contain entries from multiple distinct prefixes")
    g.add_argument("--safe-substring", default=DEFAULT_SAFE_SUBSTRING,
                   help=f"Override safe substring (default: '{DEFAULT_SAFE_SUBSTRING}'). "
                        "Must not be empty unless --allow-unsafe-prefix.")

    # Objects
    g = p.add_argument_group("objects")
    g.add_argument("--object-kind", choices=["parquet", "index", "index-versioned"],
                   default="parquet", help="Object kind (default: parquet)")
    g.add_argument("--index-version", type=int, default=None,
                   help="For index-versioned, e.g. 1")
    g.add_argument("--count", type=int, default=0,
                   help="Number of objects to generate (required)")
    g.add_argument("--size-bytes", type=int, default=1, dest="size_bytes",
                   help="Object content length; 0 = zero-byte (default: 1)")
    g.add_argument("--max-size-bytes", type=int, default=DEFAULT_MAX_SIZE_BYTES, dest="max_size_bytes",
                   help=f"Max allowed --size-bytes (default: {DEFAULT_MAX_SIZE_BYTES})")
    g.add_argument("--allow-large-objects", action="store_true",
                   help="Bypass --max-size-bytes cap")

    # Rate
    g = p.add_argument_group("rate / concurrency")
    g.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                   help=f"Max parallel writes/deletes (default: {DEFAULT_CONCURRENCY})")
    g.add_argument("--sleep-ms", type=int, default=0, dest="sleep_ms",
                   help="Sleep N ms between each object write (default: 0)")

    # Manifest
    g = p.add_argument_group("manifest / output")
    g.add_argument("--manifest",
                   help="Write/read generated object paths to this manifest")
    g.add_argument("--overwrite-manifest", action="store_true",
                   help="Allow overwriting an existing manifest in write mode")
    g.add_argument("--skip-count-verification", action="store_true",
                   help="Skip before/after object-count delta check (NOT RECOMMENDED)")

    return p


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = build_parser()
    args = p.parse_args(argv)

    # Post-process: --execute flips dry_run
    args.dry_run = not args.dry_run_false
    delattr(args, "dry_run_false")

    return args


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)

    # Mode-specific validation
    s3_bucket: Optional[str] = None
    s3_key_prefix: Optional[str] = None

    if args.cleanup:
        validate_cleanup_mode(args)
        region_prefix = "unknown/"

        # S3 bucket validation (pre-network-call): ensure every manifest entry
        # has the correct bucket before we touch the object store.
        if args.transport == "s3" and not args.dry_run:
            validate_s3_cleanup_bucket(args.manifest, args.bucket)
        elif args.transport == "s3":
            # Dry-run: we still pass bucket through for display but skip strict
            # network validation.  Bucket mismatch would error at execute time.
            s3_bucket = args.bucket

        # Validate manifest entries (path shape, safe substring, multi-prefix)
        validate_cleanup_manifest(
            args.manifest,
            args.safe_substring,
            args.allow_unsafe_prefix,
            args.allow_multiple_prefixes,
        )

        # Set s3_bucket for the execute path (validated above)
        if args.transport == "s3":
            s3_bucket = args.bucket

        # Infer prefix from first manifest entry for display
        first = ""
        with open(args.manifest, "r") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    first = line
                    break
        if "/index/" in first:
            region_prefix = first.rsplit("/index/", 1)[0] + "/"
        else:
            region_prefix = first.rsplit("/", 1)[0] + "/"
    else:
        validate_write_mode(args)

        if args.transport == "s3":
            if args.region_prefix:
                # Direct prefix: strict parse (rules depend on flags)
                s3_bucket, s3_key_prefix, display = parse_s3_region_prefix(
                    args.region_prefix,
                    has_alias=args.s3_prefix_has_alias,
                    expected_bucket=args.bucket or "",
                )
                region_prefix = display
                if not args.bucket:
                    args.bucket = s3_bucket
            else:
                # Computed prefix
                region_prefix = compute_prefix(args)
                s3_bucket = args.bucket or ""
                if s3_bucket and region_prefix.startswith(s3_bucket + "/"):
                    s3_key_prefix = region_prefix[len(s3_bucket) + 1:]
                else:
                    s3_key_prefix = region_prefix
        else:
            region_prefix = compute_prefix(args)

    # Build S3 info tuple for display (bucket, key_prefix)
    s3_display_info: Optional[Tuple[str, str]] = None
    if args.transport == "s3" and s3_bucket and s3_key_prefix is not None:
        s3_display_info = (s3_bucket, s3_key_prefix)

    # Display summary
    print_summary(args, region_prefix, s3_display_info)

    # Prefix guard
    if not check_prefix_guard(region_prefix, args.safe_substring, args.allow_unsafe_prefix):
        sys.exit(1)
    print()

    # Verify existing region objects (write mode only)
    if not args.cleanup:
        verified = verify_existing_region_object(args, region_prefix, s3_bucket, s3_key_prefix)
        if verified and not args.i_verified_prefix:
            args.i_verified_prefix = True
        print()

    # Execute
    if args.cleanup:
        do_cleanup(args, s3_bucket)
    else:
        do_write(args, region_prefix, s3_bucket, s3_key_prefix)


if __name__ == "__main__":
    main()
