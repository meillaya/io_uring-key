#!/usr/bin/env python3
"""Generate p50/p95/p99 latency + throughput charts from YCSB CSV output.

Usage:
    python3 tools/charts.py --input results/ --output charts/

Expects CSV files with columns: backend, workload, op, p50_us, p95_us, p99_us, throughput_ops
"""

import argparse
import csv
import os
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Generate YCSB benchmark charts")
    parser.add_argument("--input", required=True, help="Directory with CSV results")
    parser.add_argument("--output", default="charts", help="Output directory for PNGs")
    args = parser.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: matplotlib required. Install: pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    # TODO: G3 implementation — parse CSVs, generate comparative charts
    print(f"Found {len(csv_files)} CSV file(s). Chart generation pending G3 implementation.")


if __name__ == "__main__":
    main()
