# kernel-v2

An io_uring-based log-structured key-value store in C++23.

## Features

- LSM-tree storage engine (WAL + skip-list memtable + SSTables)
- Four interchangeable I/O backends benchmarked head-to-head:
  - sync (pread/pwrite + fsync)
  - threadpool (std::thread pool dispatch)
  - mmap (memory-mapped segments)
  - io_uring (batched submit + registered buffers + fixed files)
- Redis-compatible RESP2 API (redis-cli / redis-benchmark compatible)
- Near-full Raft-lite replication over TCP
- YCSB A-F benchmark suite with p50/p95/p99 latency charts
- Crash-recovery verified via SIGKILL + I/O fault injection

## Quick Start

```bash
# Enter dev environment
devenv shell   # or: nix develop

# Build
cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build

# Test
cd build && ctest --output-on-failure
```

## Requirements

- Linux kernel ≥ 6.x (io_uring features; tested on 7.1)
- liburing ≥ 2.15
- GCC ≥ 16 or Clang ≥ 22 (C++23)
- CMake ≥ 3.28

## Resume

- Built a serious io_uring LSM KV store in C++23 with 4 interchangeable I/O backends,
  Redis RESP2 API, Raft-lite replication, YCSB benchmarks with latency charts, and
  crash-recovery verification — all tested with ctest in a reproducible Nix/devenv env.

## License

MIT
