#!/usr/bin/env bash
# Crossed-order L/N/F measurement over one shared data directory.
#
# Arms are interleaved per case rather than run as whole sweeps: the machine is
# shared, so a 3.4x swing on the never-fused control between two whole-arm sweeps
# is drift, not an effect. Each case runs L, N, F in a rotated order per round, so
# any drift lands on all three arms instead of one.
#
# L and N are the pre-fix commit with the native flag off and on; F is the same
# commit plus the per-field reset index. They are the three arms of section 4.3 of
# docs/fused-series-report.md.
set -euo pipefail

BIN_DIR=${BIN_DIR:-/mnt/nvme_rust/rust-targets/narthex-min-chain/fused-bench-bin}
OUT=${OUT:-$HOME/.cache/fused-bench/records}
DATA=${DATA:-$HOME/.cache/fused-scale/data-seed}
DRIVER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fused_series_bench.py"
ROUNDS=${ROUNDS:-3}
CASES=${CASES:-S,G,W,C,control}
rm -rf "$OUT"; mkdir -p "$OUT"

# L and N are the same binary with the flag off and on; F is the fixed build.
declare -A BIN=(
  [L]="${BIN_L:-$BIN_DIR/greptime-f387830a020}"
  [N]="${BIN_N:-$BIN_DIR/greptime-f387830a020}"
  [F]="${BIN_F:-$BIN_DIR/greptime-fixed2}"
)
declare -A ARM=([L]=off [N]=on [F]=on)
declare -A COMMIT=(
  [L]=${COMMIT_L:-f387830a020}
  [N]=${COMMIT_N:-f387830a020}
  [F]=${COMMIT_F:-f387830a020+reset-index}
)

# Each round permutes the arm order, so an arm is not always the one that follows
# another arm's server shutdown.
declare -A ORDER=(
  [1]="L N F"
  [2]="N F L"
  [3]="F L N"
)

for case in ${CASES//,/ }; do
  for round in $(seq 1 "$ROUNDS"); do
    for arm in ${ORDER[$round]}; do
      echo "=== case $case round $round arm $arm (${ARM[$arm]}, ${COMMIT[$arm]}) ==="
      python3 "$DRIVER" run \
        --data-home "$DATA" \
        --bin "${BIN[$arm]}" \
        --arm "${ARM[$arm]}" \
        --commit "${COMMIT[$arm]}" \
        --cases "$case" \
        --out "$OUT/c${case}-r${round}-${arm}.json" \
        --log "$OUT/c${case}-r${round}-${arm}.log" \
        --warmup 2 --iterations 9 2>&1 | sed "s/^/  [$arm] /"
    done
  done
done
echo "all runs complete"
