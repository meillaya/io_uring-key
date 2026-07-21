#!/usr/bin/env bash
# Generate flamegraph from perf profiling of the io_uring hot path.
#
# Usage:
#   ./tools/flamegraph.sh <binary> [args...]
#
# Requires: perf, FlameGraph scripts (Brendan Gregg)
set -euo pipefail

BINARY="${1:?Usage: $0 <binary> [args...]}"
shift

PERF_DATA="perf.data"
SVG_OUT="flamegraph.svg"

echo "Recording perf data for: ${BINARY} $*"
perf record -F 99 -g -- "${BINARY}" "$@"

echo "Generating flamegraph..."
perf script | stackcollapse-perf.pl | flamegraph.pl > "${SVG_OUT}"

echo "Flamegraph written to ${SVG_OUT}"
