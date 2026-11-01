# kernel-v2 design document

> Status: implementation notes from G0 to G10. The server does not start the
> Raft library. The benchmark skips WAL fsync by default and `--sync-wal`
> enables it. See [docs/AUDIT.md](docs/AUDIT.md) for the audit and remediation.

## Overview

A log-structured key-value store in C++23 with:
- WAL + skip-list memtable + SSTables + level-0 full compaction (LSM-tree).
- Four interchangeable I/O backends (sync, threadpool, mmap, io_uring).
- Optional TLS with OpenSSL.
- RESP2 API over TCP with a bounded command set and a hardened parser.
- Raft library (standalone, not used by the server): election, log
  replication, **persistent log + snapshots**, and a **real TCP transport**
  (POSIX sockets).

This document describes the implemented system. Partial/stubbed areas are
labeled explicitly.

## Architecture

```
┌─────────────────────────────────────────────────┐
│             RESP2 Server (src/server)           │
│      TCP, thread-per-connection (POSIX)         │
├─────────────────────────────────────────────────┤
│              Raft-lite (src/raft)               │
│   election · replication · persistent log +     │
│   snapshots · TCP transport (POSIX sockets)     │
├─────────────────────────────────────────────────┤
│               KV Engine (src/engine)            │
├──────────┬──────────┬───────────┬───────────────┤
│   WAL    │ Memtable │  SSTable  │  Compaction   │
│ (src/wal)│(src/mem) │(src/sst)  │(src/compaction)│
├──────────┴──────────┴───────────┴───────────────┤
│            I/O Backend Interface (src/io)       │
├──────────┬──────────┬───────────┬───────────────┤
│  sync    │threadpool│   mmap    │   io_uring    │
└──────────┴──────────┴───────────┴───────────────┘
```

There is **no** `src/net` io_uring network reactor. Both the RESP server and
the Raft TCP transport use plain POSIX sockets directly. The Raft row is a
library, not part of the server path. `kv_server` never starts it.

## Module Layout

| Module | Path | Responsibility |
|--------|------|----------------|
| util | `src/util/` | Aligned buffers, CRC32C, Status, histogram, logging |
| wal | `src/wal/` | Write-ahead log: group commit, CRC, replay, torn-write |
| memtable | `src/memtable/` | Concurrent skip list |
| sstable | `src/sstable/` | Sorted runs, sparse index, bloom filter, blocks |
| compaction | `src/compaction/` | Level-0 full merge |
| io | `src/io/` | I/O backend interface + 4 implementations |
| engine | `src/engine/` | LSM engine: Get/Put/Delete, flush, compaction, recovery |
| server | `src/server/` | RESP2 protocol + hardened parser + command dispatch + TCP server |
| raft | `src/raft/` | Raft-lite: election, replication, persistent log, snapshots, TCP transport |
| bench | `src/bench/` | YCSB workload generator + latency capture + CSV |

## I/O Backend Contract

`io_backend.h` is the single I/O abstraction. All four backends implement the
same interface; the engine/WAL/SSTable layers only touch I/O through it.

Interface methods:
- `Open/Close/Delete/FileSize`: file lifecycle.
- `Truncate`: cut a torn WAL tail.
- `Rename`: publish an SSTable atomically.
- `Read/Write/Append/Sync`: synchronous I/O.
- `SyncDirectory`: make creates, renames, and unlinks durable.
- `SubmitBatch`: batched I/O (default: synchronous fallback; io_uring overrides
  with true batched submission).
- Capability queries: `SupportsDirectIO/BatchedIO/RegisteredBuffers/FixedFiles`.

### io_uring backend specifics
- Registers a pool of 4096-aligned buffers (`IORING_REGISTER_BUFFERS`) and a
  fixed file table (`IORING_REGISTER_FILES`).
- Attempts `IORING_SETUP_SQPOLL` by default; if the kernel refuses (e.g. EPERM),
  it falls back to a normal submission ring.
- `prep_read_fixed`/`prep_write_fixed` are used when a request buffer is exactly
  one of the registered pool buffers; otherwise normal `prep_read`/`prep_write`
  (with fixed files when available). The engine's point-lookup path currently
  passes caller-owned buffers, so it uses the normal path (see Limitations).

## Apples-to-Apples Benchmark Contract

ONE engine, ONE memtable/SSTable/WAL/format/durability policy. The four backends
differ ONLY in the I/O submission/completion path.

Held constant: memtable (concurrent skip list), SSTable format (block-based,
sparse index, bloom filter), WAL format (append-only, per-record CRC),
compaction policy (level-0 full merge), durability policy (WAL fsync on
commit in server mode; skipped by default in the benchmark), and the
YCSB-inspired A to F key/value distribution.

## LSM Engine

- `Get` is lock-free with respect to flush: the memtable is held in a
  `std::atomic<std::shared_ptr<Memtable>>`; flush swaps in a fresh memtable with
  a release-store while concurrent readers keep their own `shared_ptr` snapshot
  (RCU-style). Verified TSan-clean by a readers-vs-flush soak test.
- `Get(key, *out)` returns a `Status` so the read path distinguishes
  **not-found** from **corruption**: an SSTable block CRC mismatch or read
  failure surfaces as `Status::Corruption` instead of being masked as
  not-found. The RESP `GET` command returns `-ERR Corruption: ...` in that case.
- `Clear()` implements `FLUSHDB`: resets the memtable, deletes every SSTable
  file, and truncates the WAL (scoped to the engine's own data dir). `DbSize()`
  counts live keys across memtable + SSTables (newest version wins).
- Compaction is invoked from the flush path once the level-0 SSTable count
  reaches four. It merges every table into one, releases `sst_mu_` while the
  merge runs, and writes the output under a temporary name before renaming it.

## RESP2 Server

- TCP, thread-per-connection. The parser (`RespParser::ParseCommandSafe`)
  returns a three-state outcome (`kOk` / `kIncomplete` / `kError`) and **never
  throws**: integers are parsed with a bounds-checked routine (no `std::stoll`),
  and bulk/array lengths are capped so a hostile client cannot trigger
  `std::bad_alloc`. Malformed input yields a `-ERR protocol error` reply and the
  connection is dropped; the server keeps running. The connection loop is also
  wrapped in `try/catch` as defense in depth.
- Command set: `PING ECHO GET SET[NX/XX] DEL EXISTS MGET MSET DBSIZE FLUSHDB
  INFO SELECT COMMAND SCAN AUTH`. `SET ... EX` is rejected; `SCAN` is a cursor
  over a live-key snapshot; `SELECT` accepts only database 0.
- `MSET` writes one CRC-protected batch record. A torn batch is discarded as a
  unit, so recovery never applies a prefix.
- Transport: optional TLS 1.2 or newer with a PEM certificate chain and key.
  Plain TCP remains the default.
- Connection controls: optional shared-password auth, a connection limit, an
  idle timeout, and a request buffer cap.

## Raft-lite Design

Implemented:
- **Leader election** with randomized timeouts and **log replication** with
  next/match-index tracking and majority commit.
- **Persistent state** (`<data_dir>/raft_state.bin`, CRC-checked, atomic
  tmp+rename, bounds-checked on load): `currentTerm`, `votedFor`, `commitIndex`,
  the log, and the latest snapshot. Persistence is a no-op when `data_dir` is
  empty (ephemeral test clusters). On restart, the committed log is replayed
  through the apply callback to rebuild the state machine.
- **Snapshots**: `TakeSnapshot` compacts applied entries into a snapshot and
  truncates the log; `InstallSnapshot` RPC lets a leader fast-forward a follower
  that is behind the compacted log (the leader ships the snapshot when a
  follower's `next_index` is at/below the snapshot base).
- **Leader-only reads**: the leader serves reads from its state machine
  (leader-lease-style; **not** a full ReadIndex heartbeat-confirm). Follower
  reads are out of scope.
- **Transports**: `InMemoryTransport` (deterministic, unit tests) and
  `TcpRaftTransport` (real TCP over POSIX sockets, length-framed binary RPCs).
- **Dynamic joint-consensus membership: STRETCH only (not implemented).**

### Read consistency (v1)
Reads are served only by the leader, directly from its in-memory state machine.
This is a leader-lease-style guarantee (the leader assumes it is still leader);
it does **not** perform a ReadIndex majority-heartbeat confirm before each read.
This is adequate for the single-cluster, leader-only v1 scope and is documented
as a limitation rather than a full linearizable-read implementation.

## Dependencies

- **Runtime:** system liburing 2.14 or newer. OpenSSL is optional for TLS.
- **Test:** doctest (header-only, via CMake FetchContent).
- **Tooling (not runtime deps):** python3 + matplotlib (charts), perf +
  FlameGraph (profiling).

## Status

- [x] G0: Scaffold and reproducible dev env (flake.nix/devenv.nix + flake.lock).
- [x] G1: Storage core on sync backend (WAL, memtable, SSTable, engine).
- [x] G2: io_uring backend (batched submit, registered buffers/files, SQPOLL opt).
- [x] G3: Backends + YCSB bench + charts (charts.py implemented).
- [x] G4: Redis RESP2 API over TCP (POSIX sockets; hardened parser). No io_uring
      net reactor (see Limitations).
- [x] G5: Raft-lite: election, replication, persistent log + snapshots,
      InstallSnapshot, restart recovery, real TCP transport (tested in-process).
- [x] G6: Hardening, docs reconciled with reality, final benchmark report.
- [x] G8: Code-review blockers resolved (RESP hardening, memtable race, FLUSHDB,
      corruption surfacing, compaction wiring, charts, Raft persistence/snapshots/
      TCP, honest docs).

## Limitations / future work

- The benchmark scan path rebuilds the live-key set on every scan. It is not a
  production iterator and does not scale with the dataset.
- Compaction is a full level-0 merge. There is no leveled compaction.
- No io_uring network reactor; networking uses plain POSIX sockets.
- Raft TCP transport is real but tested in-process; a separate multi-process
  cluster binary or harness is future work.
- Raft reads are leader-lease-style, not full ReadIndex; no follower reads.
- No dynamic Raft membership.
- `SET ... EX` is rejected; TTL is not implemented.
- TLS is optional and must be enabled at build time and runtime.
- `AUTH` is a shared password. Use TLS when the network is untrusted.
- No ACL, rate limiting, or multi-tenancy.
