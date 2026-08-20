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

"""Small, dependency-free SSE client for GreptimeDB EXPLAIN ANALYZE metrics."""

import argparse
import datetime
import json
import os
import re
import socket
import ssl
import sys
import time
from collections import OrderedDict
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener


ENDPOINT = "/v1/sql/analyze/stream"
USER_AGENT = "greptime-explain-sse/1.0 (Python stdlib)"
TERMINAL_EVENTS = {"final", "error", "canceled"}


class ClientError(Exception):
    """An expected client-side or protocol error."""


class NoRedirectHandler(HTTPRedirectHandler):
    """Refuse redirects so credentials cannot be sent to another origin."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Stream GreptimeDB EXPLAIN ANALYZE VERBOSE metrics over SSE."
    )
    parser.add_argument("--url", required=True, help="Frontend HTTP(S) base or stream URL")
    parser.add_argument("--sql", required=True, help="One explicit EXPLAIN ANALYZE VERBOSE query")
    parser.add_argument(
        "--snapshot-interval-ms", type=int, default=1000, metavar="N", help="Snapshot interval (default: 1000)"
    )
    parser.add_argument(
        "--timeout", type=float, default=660, metavar="SECONDS", help="Socket inactivity timeout (default: 660)"
    )
    parser.add_argument(
        "--header", action="append", default=[], metavar="NAME: VALUE", help="Additional request header (repeatable)"
    )
    tls = parser.add_mutually_exclusive_group()
    tls.add_argument("--insecure", action="store_true", help="Do not verify the TLS certificate")
    tls.add_argument("--ca-file", metavar="PATH", help="PEM CA bundle for TLS verification")
    parser.add_argument("--output", metavar="PATH", help="Write complete SSE events as JSONL")
    parser.add_argument("--quiet", action="store_true", help="Suppress live diagnostic output")
    args = parser.parse_args(argv)
    if not 1000 <= args.snapshot_interval_ms <= 60000:
        parser.error("--snapshot-interval-ms must be between 1000 and 60000 (inclusive)")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def stream_url(value, interval_ms):
    parts = urlsplit(value)
    if parts.scheme.lower() not in ("http", "https") or not parts.netloc:
        raise ClientError("--url must be an http(s) URL with a host")
    path = parts.path or ""
    if not path.rstrip("/").endswith(ENDPOINT):
        path = path.rstrip("/") + ENDPOINT
    parsed_query = parse_qsl(parts.query, keep_blank_values=True)
    if any(key.lower() == "sql" for key, _ in parsed_query):
        raise ClientError("--url must not contain a sql query parameter; provide SQL only through --sql")
    query = [(key, val) for key, val in parsed_query if key != "snapshot_interval_ms"]
    query.append(("snapshot_interval_ms", str(interval_ms)))
    return urlunsplit((parts.scheme, parts.netloc, path, urlencode(query), ""))


def validate_sql(sql):
    # This deliberately validates the visible statement prefix rather than rewriting
    # input.  The server remains the authority on SQL grammar and table semantics.
    match = re.match(
        r"^\s*EXPLAIN\s+ANALYZE\s+VERBOSE(?:\s+FORMAT\s+JSON)?\s+(.+?)\s*;?\s*$",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match or not match.group(1).strip():
        raise ClientError(
            "--sql must be exactly one explicit EXPLAIN ANALYZE VERBOSE statement "
            "(optionally followed by FORMAT JSON); ordinary SELECT is not rewritten"
        )
    statement = match.group(1).strip()
    if has_top_level_semicolon(statement):
        raise ClientError("--sql must contain exactly one statement (found another top-level ';')")


def has_top_level_semicolon(sql):
    quote = None
    escaped = False
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote:
            if quote == "'" and char == "'" and index + 1 < len(sql) and sql[index + 1] == "'":
                index += 2
                continue
            if escaped:
                escaped = False
            elif char == "\\" and quote in ("'", '"'):
                escaped = True
            elif char == quote:
                quote = None
        elif char in ("'", '"', "`"):
            quote = char
        elif char == ";":
            return True
        index += 1
    return False


def request_headers(values):
    headers = OrderedDict((("Accept", "text/event-stream"), ("User-Agent", USER_AGENT)))
    for value in values:
        if ":" not in value:
            raise ClientError("--header must have the form 'Name: value'")
        name, header_value = value.split(":", 1)
        name, header_value = name.strip(), header_value.lstrip()
        if not name or any(char in name + header_value for char in "\r\n"):
            raise ClientError("invalid --header name or value")
        key = next((key for key in headers if key.lower() == name.lower()), name)
        if key.lower() == "accept" and header_value.lower() != "text/event-stream":
            raise ClientError("Accept must be text/event-stream for this SSE endpoint")
        headers[key] = header_value
    return headers


def tls_context(args):
    if args.insecure:
        return ssl._create_unverified_context()
    if args.ca_file:
        return ssl.create_default_context(cafile=args.ca_file)
    return ssl.create_default_context()


def iso_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def safe_body(body, secrets=()):
    text = body[:4096].decode("utf-8", errors="replace")
    return sanitize_text(text.replace("\r", "\\r").replace("\n", "\\n"), secrets)


def sanitize_text(text, secrets):
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def json_summary(value, limit=600, secrets=()):
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError):
        text = repr(value)
    if len(text) > limit:
        return text[: limit - 3] + "..."
    return text


def item_identity(item, index):
    if isinstance(item, dict):
        for key in ("name", "operator", "plan_node", "id"):
            if key in item and item[key] is not None:
                return "%s=%s" % (key, json_summary(item[key], 120))
        # The service currently wraps operators in a stage's ``plan`` object.
        nested = item.get("plan")
        if isinstance(nested, dict):
            for key in ("name", "operator", "plan_node", "id"):
                if key in nested and nested[key] is not None:
                    return "%s=%s" % (key, json_summary(nested[key], 120))
    return "index=%d" % index


def snapshot_items(payload):
    items = payload.get("metrics") if isinstance(payload, dict) else None
    return items if isinstance(items, list) else []


def snapshot_diff(previous, current):
    def keyed(items):
        result = OrderedDict()
        for index, item in enumerate(items):
            identity = item_identity(item, index)
            # Keep duplicate operator names distinct while retaining a useful label.
            count = 1
            base = identity
            while identity in result:
                count += 1
                identity = "%s#%d" % (base, count)
            result[identity] = item
        return result

    old, new = keyed(previous), keyed(current)
    changed = [identity for identity in new if identity in old and canonical(old[identity]) != canonical(new[identity])]
    added = [identity for identity in new if identity not in old]
    removed = [identity for identity in old if identity not in new]
    identities = ([("changed", value) for value in changed] + [("new", value) for value in added] + [("removed", value) for value in removed])[:5]
    labels = ",".join("%s:%s" % pair for pair in identities) or "none"
    return "SNAPSHOT_DIFF changed=%d new=%d removed=%d identities=%s (complete snapshot; snapshot diff, not server delta)" % (
        len(changed),
        len(added),
        len(removed),
        labels,
    )


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def operator_field(item, names):
    if not isinstance(item, dict):
        return None
    for name in names:
        if name in item:
            return item[name]
    nested = item.get("plan")
    if isinstance(nested, dict):
        return operator_field(nested, names)
    return None


def print_metrics(payload, previous, elapsed, quiet):
    items = snapshot_items(payload)
    if quiet:
        return
    partial = payload.get("partial") if isinstance(payload, dict) else None
    print("%s +%dms METRICS partial=%s operators=%d" % (iso_now(), elapsed, partial, len(items)), flush=True)
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            print("  operator[%d] %s" % (index, json_summary(item)), flush=True)
            continue
        selected = OrderedDict()
        fields = {
            "name": ("name", "operator", "plan_node"),
            "rows": ("rows", "output_rows"),
            "batches": ("batches", "output_batches"),
            "elapsed": ("elapsed", "elapsed_ms"),
            "elapsed_compute": ("elapsed_compute",),
            "metrics": ("metrics",),
        }
        for label, names in fields.items():
            found = operator_field(item, names)
            if found is not None:
                selected[label] = found
        print("  operator[%d] %s" % (index, json_summary(selected or item)), flush=True)
    print(snapshot_diff(previous or [], items), flush=True)


def diagnostic(event, payload, elapsed, previous, quiet):
    if event == "metrics":
        print_metrics(payload, previous, elapsed, quiet)
    elif event == "final":
        if not quiet:
            partial = payload.get("partial") if isinstance(payload, dict) else None
            metrics = len(snapshot_items(payload))
            total = payload.get("elapsed_ms") if isinstance(payload, dict) else None
            print("%s +%dms FINAL partial=%s metrics=%d elapsed_ms=%s" % (iso_now(), elapsed, partial, metrics, total), flush=True)
    elif event in ("error", "canceled"):
        if not quiet:
            details = {}
            if isinstance(payload, dict):
                for key in ("reason", "code", "elapsed_ms"):
                    if key in payload:
                        details[key] = payload[key]
            print("%s +%dms %s %s" % (iso_now(), elapsed, event.upper(), json_summary(details or payload)), flush=True)
    elif not quiet:
        print("%s +%dms EVENT %s %s" % (iso_now(), elapsed, event, json_summary(payload)), flush=True)


def write_event(output, event, raw_data, started, quiet):
    try:
        payload = json.loads(raw_data)
    except (TypeError, ValueError):
        payload = raw_data
        print("WARNING event %s data is not valid JSON; retaining raw data" % event, file=sys.stderr, flush=True)
    record = {"received_at": iso_now(), "elapsed_ms": int((time.monotonic() - started) * 1000), "event": event, "data": payload}
    if output:
        output.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        output.flush()
    return payload, record["elapsed_ms"]


def events(response):
    event_name = "message"
    data = []
    while True:
        line = response.readline()
        if not line:
            # SSE dispatch requires a blank line.  In particular, do not turn a
            # truncated terminal frame into a successful (or terminal) event.
            return
        line = line.decode("utf-8", errors="replace").rstrip("\r\n")
        if line == "":
            if data:
                yield event_name, "\n".join(data), True
            event_name, data = "message", []
            continue
        if line.startswith(":"):
            continue
        if ":" in line:
            field, value = line.split(":", 1)
            if value.startswith(" "):
                value = value[1:]
        else:
            field, value = line, ""
        if field == "event":
            event_name = value
        elif field == "data":
            data.append(value)
        # id and retry are intentionally ignored, as are extension fields.


def open_output(path):
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as error:
        raise ClientError("cannot open --output: %s" % error) from error
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        else:
            try:
                os.chmod(path, 0o600)
            except OSError:
                # Windows permissions are best-effort and do not map directly
                # to POSIX owner/group/other bits.
                pass
        return os.fdopen(fd, "w", encoding="utf-8")
    except OSError as error:
        try:
            os.close(fd)
        except OSError:
            pass
        raise ClientError("cannot set secure permissions for --output: %s" % error) from error


def validate_terminal(event, payload):
    if event not in TERMINAL_EVENTS:
        return
    if not isinstance(payload, dict) or payload.get("state") != event:
        raise ClientError("protocol error: terminal event %r has no matching state" % event)


def run(args):
    validate_sql(args.sql)
    url = stream_url(args.url, args.snapshot_interval_ms)
    headers = request_headers(args.header)
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    body = urlencode({"sql": args.sql}).encode("utf-8")
    request = Request(url, data=body, headers=dict(headers), method="POST")
    output = None
    started = None
    secrets = [value.split(":", 1)[1].lstrip() for value in args.header if ":" in value]
    try:
        if args.output:
            output = open_output(args.output)
        context = tls_context(args)
        opener = build_opener(NoRedirectHandler, HTTPSHandler(context=context))
        started = time.monotonic()
        try:
            response = opener.open(request, timeout=args.timeout)
        except HTTPError as error:
            if 300 <= error.code < 400:
                raise ClientError("redirect refused; use final HTTPS endpoint") from error
            raise
        with response:
            status = getattr(response, "status", response.getcode())
            content_type = response.headers.get("Content-Type", "")
            if status < 200 or status >= 300:
                raise ClientError("HTTP status %s; body=%s" % (status, safe_body(response.read(4097), secrets)))
            if not content_type.lower().split(";", 1)[0].strip() == "text/event-stream":
                raise ClientError(
                    "expected Content-Type text/event-stream, got %r; body=%s"
                    % (content_type, safe_body(response.read(4097), secrets))
                )
            previous = None
            last_event = "none"
            terminal = None
            try:
                for event, raw_data, complete in events(response):
                    payload, elapsed = write_event(output, event, raw_data, started, args.quiet)
                    last_event = event
                    if event in TERMINAL_EVENTS:
                        validate_terminal(event, payload)
                    diagnostic(event, payload, elapsed, previous, args.quiet)
                    if event == "metrics" and isinstance(payload, dict):
                        previous = snapshot_items(payload)
                    if event in TERMINAL_EVENTS:
                        terminal = event
                        break
            except (socket.timeout, TimeoutError):
                message = "stream inactivity timeout after %ss; last_event=%s; elapsed_ms=%d" % (
                    args.timeout, last_event, int((time.monotonic() - started) * 1000)
                )
                if not args.quiet:
                    print("TIMEOUT %s (last metrics snapshot was retained if --output was used)" % message, file=sys.stderr, flush=True)
                return 1
            if terminal == "final":
                return 0
            if terminal in ("error", "canceled"):
                return 1
            if not args.quiet:
                print("EOF without terminal event; last complete event=%s" % last_event, file=sys.stderr, flush=True)
            return 1
    except HTTPError as error:
        try:
            detail = safe_body(error.read(4097), secrets)
        except (OSError, socket.timeout):
            detail = "<body unavailable>"
        print("HTTP error %s; body=%s" % (error.code, detail), file=sys.stderr, flush=True)
        return 1
    except (URLError, OSError, ssl.SSLError, ClientError) as error:
        message = sanitize_text(str(error), secrets)
        if started is not None:
            message = "%s (elapsed_ms=%d)" % (message, int((time.monotonic() - started) * 1000))
        print("ERROR %s" % message, file=sys.stderr, flush=True)
        return 1
    finally:
        if output:
            output.close()


def main(argv=None):
    try:
        args = parse_args(argv)
        return run(args)
    except ClientError as error:
        print("ERROR %s" % error, file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
