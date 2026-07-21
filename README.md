# kernel-v2

A log-structured key-value store in C++23 with a pluggable I/O backend layer
(including an io_uring backend), a Redis-compatible RESP2 API, YCSB benchmarks,
and a Raft-lite replication layer with a persistent log, snapshots, and a real
TCP transport.

> **Honesty note.** This README describes exactly what the code does today.
> Features that are partial, stubbed, or future work are called out explicitly
> in [Limitations / future work](#limitations--future-work).

## Architecture

```
┌─────────────────────────────────────────────────┐
│        Redis RESP2 Server (src/server)          │
│   TCP, thread-per-connection, hardened parser   │
├─────────────────────────────────────────────────┤
│        Raft-lite Replication (src/raft)         │
│  election · log replication · persistent log +  │
│  snapshots · TCP transport (POSIX sockets)      │
├─────────────────────────────────────────────────┤
│              KV Engine (src/engine)             │
├──────────┬──────────┬───────────┬───────────────┤
│   WAL    │ Memtable │  SSTable  │  Compaction   │
│ (src/wal)│(src/mem) │(src/sst)  │(src/compaction)│
├──────────┴──────────┴───────────┴───────────────┤
│        I/O Backend Interface (FROZEN, src/io)   │
├──────────┬──────────┬───────────┬───────────────┤
│  sync    │threadpool│   mmap    │   io_uring    │
│ pread/   │std::thread│ mmap'd   │ batched submit│
│ pwrite   │  pool    │ segments │ +reg buf/files│
│ +fsync   │          │          │ +SQPOLL(opt)  │
└──────────┴──────────┴───────────┴───────────────┘
```

There is **no** separate io_uring network reactor. The RESP server and the Raft
TCP transport both use plain POSIX sockets directly (the server is
thread-per-connection; Raft uses short-lived request/reply connections).

## Features (as implemented)

- **LSM-tree storage engine**: append-only WAL with per-record CRC, group
  commit, replay and torn-write detection; concurrent skip-list memtable;
  SSTables with a block format, sparse index, bloom filter and footer;
  size-tiered compaction **wired into the flush path** (triggers when the
  level-0 SSTable count reaches the compaction threshold).
- **Four interchangeable I/O backends**, benchmarked head-to-head:
  - `sync` — pread/pwrite + fsync (baseline).
  - `threadpool` — std::thread pool dispatching the same synchronous ops.
  - `mmap` — memory-mapped segment files.
  - `io_uring` — liburing 2.15: batched `SubmitBatch`, registered buffers
    (`IORING_REGISTER_BUFFERS`) and registered files (`IORING_REGISTER_FILES`),
    `IORING_SETUP_SQPOLL` **attempted by default with graceful fallback**, and
    O_DIRECT-aware opens. Registered-buffer `prep_read_fixed`/`prep_write_fixed`
    are used when a caller passes one of the backend's pool buffers.
- **Redis-compatible RESP2 API** over TCP: `PING`, `ECHO`, `GET`, `SET`
  (`NX`/`XX` honored; `EX` is parsed but **not enforced** — no TTL/expiry),
  `DEL`, `EXISTS`, `MGET`, `MSET`, `DBSIZE` (real live-key count), `FLUSHDB`
  (real: clears the memtable, deletes all SSTables, truncates the WAL),
  `INFO`, `SELECT` (single DB, always OK), `COMMAND`, and a **minimal `SCAN`
  stub** (returns cursor 0 and an empty list). The parser is hardened:
  malformed input (bad integers, oversized bulk/array lengths, garbage type
  bytes) yields a `-ERR` reply and never crashes the server.
- **Raft-lite replication**:
  - Leader election and log replication with a KV state machine.
  - **Persistent state**: `currentTerm`, `votedFor`, `commitIndex`, the log,
    and the latest snapshot are persisted to `<data_dir>/raft_state.bin`
    (CRC-checked, atomic tmp+rename). On restart the committed log is replayed
    through the apply callback to rebuild state.
  - **Snapshots**: log compaction into a snapshot with log truncation
    (`TakeSnapshot`), plus an `InstallSnapshot` RPC so a leader can fast-forward
    a follower that is behind the compacted log.
  - **Leader-only reads**: the leader serves reads from its state machine.
    This is a leader-lease-style approach (the leader serves reads because it
    believes it is leader); it is **not** a full ReadIndex heartbeat-confirm.
  - **Two transports**: a deterministic in-memory transport (unit tests) and a
    **real TCP transport over POSIX sockets** (`src/raft/tcp_transport.h`),
    exercised by an in-process 3-node localhost cluster test that covers
    election, replication, and failover after killing the leader.
- **YCSB A–F benchmark suite** with p50/p95/p99 latency histograms, CSV export,
  and `tools/charts.py` (matplotlib) that renders per-workload p50/p95/p99
  latency charts (overall/read/write) plus a throughput chart.
- **Engine crash-recovery** verified via SIGKILL during writes plus WAL CRC
  torn-write detection, all in isolated `mktemp` data dirs.

## Quick Start

```bash
# Enter dev environment
devenv shell   # or: nix develop

# Build
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j

# Test (64 tests)
ctest --test-dir build --output-on-failure

# Run server
./build/kv_server --port 6379 --backend sync --data-dir /tmp/kv_data

# Run benchmark (writes benchmark_results.csv)
./build/kv_bench --keys 1000 --ops 5000 --output benchmark_results.csv

# Generate charts (requires matplotlib: pip install matplotlib)
python3 tools/charts.py --input benchmark_results.csv --output charts/
```

## Requirements

- Linux kernel with io_uring (developed/tested on 7.1). The io_uring backend
  degrades gracefully (SQPOLL fallback) where features are unavailable.
- liburing ≥ 2.15 (system package).
- GCC ≥ 16 or Clang ≥ 22 (C++23).
- CMake ≥ 3.28.
- Python 3 + matplotlib (for charts only — not a runtime dependency).

## Benchmark Results

`./build/kv_bench --keys 1000 --ops 5000` on the dev host (kernel 7.1). Average
throughput across YCSB A–F:

| backend    | avg ops/sec | vs sync |
|------------|-------------|---------|
| sync       | ~286k       | 1.00x   |
| mmap       | ~262k       | 0.91x   |
| io_uring   | ~217k       | 0.76x   |
| threadpool | ~197k       | 0.69x   |

**Reading these numbers honestly:** at this tiny, fully cached scale with
synchronous per-operation submission and an fsync per write, plain `pread`/
`pwrite` (the `sync` backend) is the fastest. The io_uring backend enables
SQPOLL and registers buffers/files, and it **beats sync on the read-only
workload C** (~506k vs ~390k, ~1.30x) where there is no per-write fsync, but it
trails on write-heavy workloads because the engine's hot path still submits one
I/O at a time and waits. io_uring's structural advantage appears at higher
queue depths, larger I/O, and genuinely batched/async submission — see
[Limitations](#limitations--future-work).

Apples-to-apples contract: ONE engine, ONE memtable/SSTable/WAL/format/durability
policy. The four backends differ ONLY in the I/O submission/completion path.

## Test Coverage

66 ctest cases, all green (`ctest --test-dir build`). Highlights:

| Category | Coverage |
|----------|----------|
| Util | Status, CRC32, AlignedBuffer, histogram |
| WAL | append, replay, torn-write, CRC, group commit, truncate |
| Engine differential | randomized ops vs `std::map` |
| Crash recovery | SIGKILL during writes / after flush |
| io_uring | init, differential, recovery, batch, alignment, buffered, registered-buffer fast path |
| Backends | differential across sync/threadpool/mmap/io_uring |
| RESP2 | parse/serialize/roundtrip + **malformed-input hardening** |
| Server | **malformed RESP over a real socket never crashes the server** |
| Engine features | **FLUSHDB/Clear, DBSIZE, compaction wiring, corruption detection** |
| Concurrency | **readers vs flush memtable-swap soak (TSan-clean)** |
| Raft | election, replication, leader-only reads, **persistent-log restart recovery, snapshot + restart, InstallSnapshot + snapshot-base safety** (Raft layer TSan-clean) |
| Raft TCP | **3-node localhost cluster: election, replication, failover over real TCP** |

## Module Layout

| Module | Path | Description |
|--------|------|-------------|
| util | `src/util/` | Aligned buffers, CRC32C, Status, histogram, logging |
| wal | `src/wal/` | Write-ahead log: group commit, CRC, replay, torn-write |
| memtable | `src/memtable/` | Concurrent skip list |
| sstable | `src/sstable/` | Sorted runs, sparse index, bloom filter, blocks |
| compaction | `src/compaction/` | Size-tiered compaction |
| io | `src/io/` | I/O backend interface (FROZEN) + 4 implementations |
| engine | `src/engine/` | LSM engine: Get/Put/Delete, flush, compaction, recovery |
| server | `src/server/` | RESP2 protocol + hardened parser + TCP server |
| raft | `src/raft/` | Raft-lite: election, replication, persistent log, snapshots, TCP transport |
| bench | `src/bench/` | YCSB workload generator + latency capture + CSV |
| tools | `tools/` | `charts.py` (matplotlib charts from benchmark CSV) |

## Limitations / future work

Stated plainly so the docs never overclaim:

- **io_uring performance at tiny cached scale.** With 1k keys / 5k cached ops
  and synchronous one-op-at-a-time submission + per-write fsync, io_uring is
  ~0.76x of `sync` on average (faster only on read-only workload C). The
  registered-buffer fast path exists but the engine's point-lookup path passes
  caller-owned buffers, so it does not currently route through
  `prep_read_fixed`. Winning here would require batching multiple I/Os per
  submission and/or routing the read/flush path through registered pool buffers
  at higher queue depth — future work.
- **No io_uring network reactor.** Networking (RESP server and Raft transport)
  uses plain POSIX sockets, not multishot io_uring accept/recv/send.
- **Raft transport is real TCP but tested in-process.** `TcpRaftTransport`
  speaks real TCP over localhost and is tested with 3 in-process nodes
  (election, replication, failover). A separate multi-OS-process cluster binary
  / harness is future work.
- **Raft reads are leader-lease-style, not full ReadIndex.** The leader serves
  reads from its state machine without a ReadIndex heartbeat-confirm round.
  Follower reads are out of scope.
- **No dynamic Raft membership** (joint consensus) — stretch only.
- **RESP `SET ... EX` TTL is parsed but not enforced** (no key expiry).
  **`SCAN` is a minimal stub** (returns cursor 0 + empty list). `SELECT` is a
  single-DB no-op.
- **Production hardening** (TLS/auth/ACL, rate limiting, multi-tenancy) is out
  of scope.

## License

MIT
