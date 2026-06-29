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

"""Helm post-renderer for the GC stress lab GreptimeDBCluster manifest.

The cached greptimedb-cluster chart 0.8.21 always renders
``spec.monitoring.ttl`` when ``monitoring.enabled=true``.  The currently
installed shared GreptimeDB operator CRD in the office cluster rejects that
field.  This post-renderer removes only the unsupported monitoring TTL line
from the rendered GreptimeDBCluster YAML while preserving other TTL fields such
as frontend slow-query TTL.

Usage:

    helm template ... --post-renderer ./drop_monitoring_ttl_post_renderer.py
    helm upgrade ... --post-renderer ./drop_monitoring_ttl_post_renderer.py
"""

from __future__ import annotations

import sys


def main() -> int:
    in_monitoring = False

    for line in sys.stdin:
        stripped = line.strip()

        if line.startswith("  monitoring:"):
            in_monitoring = True
            sys.stdout.write(line)
            continue

        if in_monitoring:
            # Leave the spec.monitoring block when indentation returns to the
            # next top-level spec key.  YAML list/doc markers also end it.
            if stripped and not line.startswith("    "):
                in_monitoring = False
            elif line.startswith("    ttl:"):
                continue

        sys.stdout.write(line)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
