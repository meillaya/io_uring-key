# kernel-v2 — Design Document

## Overview

An io_uring-based log-structured key-value store in C++23 with:
- WAL + skip-list memtable + SSTables (LSM-tree architecture)
- Four interchangeable I/O backends (sync, threadpool, mmap, io_uring)
- Redis-compatible RESP2 API
- Near-full Raft-lite replication over TCP

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  RESP2 Server                    │
│              (src/server, src/net)               │
├─────────────────────────────────────────────────┤
│                  Raft-lite                       │
│                 (src/raft)                       │
├─────────────────────────────────────────────────┤
│                  KV Engine                       │
│                (src/engine)                      │
├──────────┬──────────┬───────────┬───────────────┤
│   WAL    │ Memtable │  SSTable  │  Compaction   │
│ (src/wal)│(src/mem) │(src/sst)  │(src/compaction)│
├──────────┴──────────┴───────────┴───────────────┤
│              I/O Backend Interface               │
│                (src/io)                          │
├──────────┬──────────┬───────────┬───────────────┤
│  sync    │threadpool│   mmap    │   io_uring    │
└──────────┴──────────┴───────────┴───────────────┘
```

## Module Layout

| Module | Path | Responsibility |
|--------|------|----------------|
| util | src/util/ | Aligned buffers, CRC32C, Status, histogram, logging |
| wal | src/wal/ | Write-ahead log with group commit, CRC, replay |
| memtable | src/memtable/ | Concurrent skip list |
| sstable | src/sstable/ | Sorted runs, sparse index, bloom filter, blocks |
| compaction | src/compaction/ | Size-tiered compaction |
| io | src/io/ | I/O backend interface + 4 implementations |
| engine | src/engine/ | LSM engine: Get/Put/Delete, recovery |
| net | src/net/ | io_uring network reactor (G6 optimization) |
| server | src/server/ | RESP2 protocol + command dispatch |
| raft | src/raft/ | Raft-lite: election, replication, snapshots |
| bench | src/bench/ | YCSB workload generator + latency capture |

## I/O Backend Contract (FROZEN at end of G1)

The `io_backend.h` interface is the single abstraction point. All four backends
implement the same interface. The engine, WAL, SSTable layers never touch I/O
directly — only through the backend interface.

Interface methods:
- `Open/Close/Delete/FileSize` — file lifecycle
- `Read/Write/Append/Sync` — synchronous I/O
- `SubmitBatch` — batched/async I/O (default: sync fallback)
- Capability queries: `SupportsDirectIO/BatchedIO/RegisteredBuffers/FixedFiles`

## Apples-to-Apples Benchmark Contract

ONE engine, ONE memtable/SSTable/WAL/format/durability policy.
The four backends differ ONLY in the I/O submission/completion path.

Held constant:
- Memtable implementation (concurrent skip list)
- SSTable format (block-based, sparse index, bloom filter)
- WAL format (append-only, per-record CRC)
- Compaction policy (size-tiered)
- Durability policy (fsync on WAL commit)
- Key/value distribution (YCSB workloads A-F)

## Raft-lite Design

- Leader-only reads (ReadIndex or leader lease) — v1 constraint
- Persistent log with CRC (stubbed for v1; disk persistence is G6)
- Log-compaction snapshots (install + truncation)
- Transport interface: TCP impl (production) + deterministic in-memory (test)
- Dynamic joint-consensus membership: STRETCH only

## Raft-lite Read Consistency (v1)

**v1 reads are leader-only.** All client reads are served by the Raft leader
using either ReadIndex or a leader lease mechanism. This guarantees linearizable
reads without the complexity of follower reads.

- **ReadIndex:** The leader confirms it is still the leader by exchanging
  heartbeats with a majority of followers before serving a read.
- **Leader lease:** The leader assumes it remains the leader for a bounded
  time after receiving a majority heartbeat response.

Follower reads are explicitly **OUT of scope** for v1.

**Dynamic joint-consensus membership changes are STRETCH only.**

## Dependencies

- **Runtime:** system liburing 2.15
- **Test:** doctest (header-only, via CMake FetchContent)
- **Tooling:** python3 + matplotlib (charts), perf + FlameGraph (profiling)

## Status

- [x] G0: Scaffold and reproducible dev env
- [x] G1: Storage core on sync backend
- [x] G2: io_uring backend
- [x] G3: Backends + YCSB bench + charts + flamegraphs
- [x] G4: Redis RESP2 API + shared net reactor
- [x] G5: Raft-lite near-full over TCP
- [x] G6: Hardening, docs, final benchmark report
