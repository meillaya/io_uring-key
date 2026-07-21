# kernel-v2

An io_uring-based log-structured key-value store in C++23 with Redis-compatible API and Raft-lite replication.

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Redis RESP2 Server                  │
│         (src/server, TCP multi-threaded)         │
├─────────────────────────────────────────────────┤
│              Raft-lite Replication               │
│    (src/raft: election, replication, snapshots)  │
├─────────────────────────────────────────────────┤
│                KV Engine (LSM)                   │
│              (src/engine)                        │
├──────────┬──────────┬───────────┬───────────────┤
│   WAL    │ Memtable │  SSTable  │  Compaction   │
│ (src/wal)│(src/mem) │(src/sst)  │(src/compaction)│
├──────────┴──────────┴───────────┴───────────────┤
│           I/O Backend Interface (FROZEN)         │
│                (src/io)                          │
├──────────┬──────────┬───────────┬───────────────┤
│  sync    │threadpool│   mmap    │   io_uring    │
│ pread/   │std::thread│ mmap'd   │ batched submit│
│ pwrite   │  pool    │ segments │ +reg buf/files│
└──────────┴──────────┴───────────┴───────────────┘
```

## Features

- **LSM-tree storage engine**: WAL + concurrent skip-list memtable + SSTables (block format, sparse index, bloom filter) + size-tiered compaction
- **Four interchangeable I/O backends** benchmarked head-to-head:
  - `sync` — pread/pwrite + fsync (baseline)
  - `threadpool` — std::thread pool dispatching sync ops
  - `mmap` — memory-mapped segment files
  - `io_uring` — batched submit + IORING_REGISTER_BUFFERS + IORING_REGISTER_FILES + optional SQPOLL + O_DIRECT
- **Redis-compatible RESP2 API**: PING, ECHO, GET, SET[NX/XX/EX], DEL, EXISTS, MGET, MSET, DBSIZE, FLUSHDB, SCAN, INFO, SELECT, COMMAND
- **Raft-lite replication**: leader election, log replication, KV state-machine, leader-only reads (ReadIndex/lease), deterministic in-memory transport for testing
- **YCSB A-F benchmark suite** with p50/p95/p99 latency histograms and CSV export
- **Crash-recovery verified** via SIGKILL + WAL CRC torn-write detection in isolated temp data dirs

## Quick Start

```bash
# Enter dev environment
devenv shell   # or: nix develop

# Build
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build

# Test (43 tests)
cd build && ctest --output-on-failure

# Run server
./build/kv_server --port 6379 --backend sync --data-dir /tmp/kv_data

# Run benchmark
./build/kv_bench --output results.csv --keys 10000 --ops 50000

# Generate charts
python3 tools/charts.py --input results.csv --output charts/
```

## Requirements

- Linux kernel ≥ 6.x (io_uring features; tested on 7.1)
- liburing ≥ 2.15 (system package)
- GCC ≥ 16 or Clang ≥ 22 (C++23)
- CMake ≥ 3.28
- Python 3 + matplotlib (for charts)

## Benchmark Results

Run `./build/kv_bench` to generate comparative results across all four backends on YCSB A-F workloads. The benchmark produces a CSV with p50/p95/p99 latency and throughput for each backend/workload combination.

Apples-to-apples contract: ONE engine, ONE memtable/SSTable/WAL/format/durability policy. The four backends differ ONLY in the I/O submission/completion path.

## Test Coverage

| Category | Tests | Status |
|----------|-------|--------|
| Util (Status, CRC32, AlignedBuffer, Histogram) | 5 | ✅ |
| WAL (append, replay, torn-write, CRC, group commit, truncate) | 5 | ✅ |
| Differential vs std::map (sync backend) | 2 | ✅ |
| Crash recovery (SIGKILL during writes, after flush) | 2 | ✅ |
| io_uring (init, differential, recovery, batch, alignment, buffered) | 6 | ✅ |
| Backend differential (sync, threadpool, mmap, io_uring) | 4 | ✅ |
| RESP2 protocol (parse, serialize, roundtrip) | 13 | ✅ |
| Raft (single-node election, 3-node election, replication, reads) | 4 | ✅ |
| Scaffold (hello world, C++23 check) | 2 | ✅ |
| **Total** | **43** | **✅** |

## Resume

- Built a serious io_uring LSM KV store in C++23 with 4 interchangeable I/O backends
  (sync, threadpool, mmap, io_uring with registered buffers/files), Redis RESP2 API,
  Raft-lite replication (election, log replication, leader-only reads), YCSB A-F
  benchmarks with latency histograms, and crash-recovery verification — all tested
  with 43 ctest cases in a reproducible Nix/devenv environment.

## Module Layout

| Module | Path | Description |
|--------|------|-------------|
| util | `src/util/` | Aligned buffers, CRC32C, Status, histogram, logging |
| wal | `src/wal/` | Write-ahead log with group commit, CRC, replay |
| memtable | `src/memtable/` | Concurrent skip list |
| sstable | `src/sstable/` | Sorted runs, sparse index, bloom filter, blocks |
| compaction | `src/compaction/` | Size-tiered compaction |
| io | `src/io/` | I/O backend interface (FROZEN) + 4 implementations |
| engine | `src/engine/` | LSM engine: Get/Put/Delete, recovery |
| server | `src/server/` | RESP2 protocol + command dispatch + TCP server |
| raft | `src/raft/` | Raft-lite: election, replication, leader-only reads |
| bench | `src/bench/` | YCSB workload generator + latency capture |

## License

MIT
