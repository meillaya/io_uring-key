#!/usr/bin/env python3
"""Generate comparative latency + throughput charts from YCSB benchmark CSV output.

Reads the CSV produced by `kv_bench` (default: benchmark_results.csv) whose
columns are:

    backend, workload, total_ops, reads, writes, elapsed_sec, throughput_ops,
    overall_p50_us, overall_p95_us, overall_p99_us,
    read_p50_us, read_p95_us, read_p99_us,
    write_p50_us, write_p95_us, write_p99_us

Emits, into the output directory:
    latency_<workload>.png  — per-workload overall p50/p95/p99 grouped by backend
    latency_read_<wl>.png   — per-workload READ p50/p95/p99 grouped by backend
    latency_write_<wl>.png  — per-workload WRITE p50/p95/p99 grouped by backend
    throughput.png          — throughput (ops/sec) per workload grouped by backend

Usage:
    python3 tools/charts.py --input benchmark_results.csv --output charts/
    python3 tools/charts.py --input results_dir/ --output charts/   # globs *.csv
"""

import argparse
import csv
import sys
from pathlib import Path

# Percentile column groups: (label prefix, p50 col, p95 col, p99 col)
PERCENTILE_GROUPS = [
    ("latency", "overall_p50_us", "overall_p95_us", "overall_p99_us"),
    ("latency_read", "read_p50_us", "read_p95_us", "read_p99_us"),
    ("latency_write", "write_p50_us", "write_p95_us", "write_p99_us"),
]


def load_rows(input_path: Path):
    """Load benchmark rows from a CSV file or a directory of CSV files."""
    if input_path.is_dir():
        csv_files = sorted(input_path.glob("*.csv"))
        if not csv_files:
            sys.exit(f"ERROR: no CSV files found in {input_path}")
    else:
        if not input_path.exists():
            sys.exit(f"ERROR: input CSV not found: {input_path}")
        csv_files = [input_path]

    rows = []
    for cf in csv_files:
        with open(cf, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    if not rows:
        sys.exit("ERROR: no data rows found in input CSV(s)")
    return rows


def to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def organize(rows):
    """Return (workloads, backends, data) where data[workload][backend] = row."""
    workloads, backends = [], []
    data = {}
    for row in rows:
        wl = row.get("workload", "?").strip()
        be = row.get("backend", "?").strip()
        if wl not in data:
            data[wl] = {}
            workloads.append(wl)
        if be not in backends:
            backends.append(be)
        data[wl][be] = row
    return workloads, backends, data


def make_latency_chart(plt, workload, backends, data, group, out_dir: Path):
    label, p50c, p95c, p99c = group
    import numpy as np

    present = [b for b in backends if b in data.get(workload, {})]
    if not present:
        return None
    rowmap = data[workload]

    p50 = [to_float(rowmap[b].get(p50c)) for b in present]
    p95 = [to_float(rowmap[b].get(p95c)) for b in present]
    p99 = [to_float(rowmap[b].get(p99c)) for b in present]

    x = np.arange(len(present))
    width = 0.26
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(x - width, p50, width, label="p50")
    ax.bar(x, p95, width, label="p95")
    ax.bar(x + width, p99, width, label="p99")
    ax.set_ylabel("Latency (µs)")
    ax.set_title(f"YCSB {workload} — {label} p50/p95/p99 by backend")
    ax.set_xticks(x)
    ax.set_xticklabels(present, rotation=20, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = out_dir / f"{label}_{workload}.png"
    fig.savefig(out, dpi=110)
    plt.close(fig)
    return out


def make_throughput_chart(plt, workloads, backends, data, out_dir: Path):
    import numpy as np

    present_be = [b for b in backends if any(b in data.get(w, {}) for w in workloads)]
    if not present_be or not workloads:
        return None
    x = np.arange(len(workloads))
    width = 0.8 / max(len(present_be), 1)
    fig, ax = plt.subplots(figsize=(9, 5))
    for i, be in enumerate(present_be):
        vals = [to_float(data.get(w, {}).get(be, {}).get("throughput_ops")) for w in workloads]
        ax.bar(x + (i - (len(present_be) - 1) / 2) * width, vals, width, label=be)
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("YCSB throughput by workload and backend")
    ax.set_xticks(x)
    ax.set_xticklabels([f"YCSB {w}" for w in workloads])
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = out_dir / "throughput.png"
    fig.savefig(out, dpi=110)
    plt.close(fig)
    return out


def main():
    parser = argparse.ArgumentParser(description="Generate YCSB benchmark charts")
    parser.add_argument("--input", default="benchmark_results.csv",
                        help="Benchmark CSV file or directory of CSVs "
                             "(default: benchmark_results.csv)")
    parser.add_argument("--output", default="charts",
                        help="Output directory for PNGs (default: charts)")
    args = parser.parse_args()

    try:
        import matplotlib
        matplotlib.use("Agg")  # headless backend; works without a display
        import matplotlib.pyplot as plt
    except ImportError:
        print("ERROR: matplotlib is required to generate charts.\n"
              "       Install it with:  pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    try:
        import numpy  # noqa: F401  (matplotlib ships numpy, but be explicit)
    except ImportError:
        print("ERROR: numpy is required (installed with matplotlib).\n"
              "       Install with:  pip install matplotlib", file=sys.stderr)
        sys.exit(1)

    rows = load_rows(Path(args.input))
    workloads, backends, data = organize(rows)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    produced = []
    for wl in workloads:
        for group in PERCENTILE_GROUPS:
            out = make_latency_chart(plt, wl, backends, data, group, out_dir)
            if out:
                produced.append(out)
    out = make_throughput_chart(plt, workloads, backends, data, out_dir)
    if out:
        produced.append(out)

    print(f"Read {len(rows)} rows: {len(workloads)} workload(s) x {len(backends)} backend(s).")
    print(f"Wrote {len(produced)} chart(s) to {out_dir}/")
    for p in produced:
        print(f"  - {p}")


if __name__ == "__main__":
    main()
