<h1 align="center" style="letter-spacing: 0.12em;">io_uring-key</h1>

<p align="center">A C++23 log-structured key-value store with a RESP2 server, four I/O backends, and a standalone Raft library.</p>

<br>

## What this is

`kv_server` is a single-process key-value server. It speaks RESP2 over TCP and
stores data in an LSM tree: a write-ahead log, a skip-list memtable, block-based
SSTables, and size-tiered compaction. Four backends implement one I/O interface:
`sync`, `threadpool`, `mmap`, and `io_uring`.

The repository also contains a Raft consensus library, a YCSB-style benchmark
harness, and a chart script. The server does not start the Raft library. The
source and runtime messages use the internal name `kernel-v2`.

This is a research prototype for systems programmers. It has known
correctness gaps. Read [Limits](#limits) before you trust it with data.

## Requirements

The dev shell supplies liburing 2.14, GCC 16, Clang 22, CMake 4.1, Python 3, and
matplotlib. Without Nix you need Linux with io_uring, liburing 2.14 or newer,
CMake 3.28 or newer, and GCC 16 or Clang 22.

## Quick start

Linux only. The flake has dev shells for `x86_64-linux` and `aarch64-linux`;
there is no macOS dev shell.

```bash
nix develop
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Start the server in one terminal:

```bash
./build/kv_server --port 6379 --backend sync --data-dir /tmp/kv_data
```

Use the Redis CLI that the dev shell provides:

```bash
redis-cli -p 6379 SET hello world
redis-cli -p 6379 GET hello
```

Run the benchmark and render its charts:

```bash
./build/kv_bench --keys 1000 --ops 5000 --output benchmark_results.csv
python3 tools/charts.py --input benchmark_results.csv --output charts/
```

## Architecture

![Architecture](docs/figures/architecture.png)

`kv_server` reads and writes through the LSM engine and one I/O backend. The
Raft code is a separate library in `src/raft`; the server never starts it. The
figure source and render command are in
[docs/figures/README.md](docs/figures/README.md).

## What works

- LSM engine: CRC-checked WAL, skip-list memtable, block SSTables with sparse
  indexes and bloom filters, and size-tiered compaction.
- I/O backends: `sync`, `threadpool`, `mmap`, and `io_uring`. The io_uring
  backend supports batched submission, registered buffers and files, and
  optional SQPOLL with a fallback. The engine still submits one operation at a
  time on its hot path.
- RESP2 server: `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`, `MGET`, `MSET`,
  `DBSIZE`, `FLUSHDB`, `INFO`, `SELECT`, `COMMAND`, and a stub `SCAN`.
- Raft library: elections, log replication, a persistent log, snapshots, and a
  TCP transport. Unit and in-process cluster tests cover it.
- Benchmarks: a YCSB-style A to F generator, p50/p95/p99 capture, CSV export,
  and matplotlib charts.

## Limits

- A torn WAL tail is never truncated. Writes accepted after recovery can be
  lost on the next restart. Mixed-size compaction can return stale values, and
  a corrupt bloom filter can crash reads. [docs/AUDIT.md](docs/AUDIT.md) gives
  each one a repro.
- `SET ... EX` parses a TTL and does not enforce it. `SCAN` returns an empty
  list. `SELECT` ignores its argument.
- `SET NX/XX`, `MSET`, and `DEL` are not atomic. `MSET` ignores write failures.
- The Raft library is not part of the server. The leader serves reads without a
  ReadIndex confirmation.
- `Server::Stop()` can block on an idle client. There is no TLS, auth, or
  connection limit.
- Networking uses POSIX sockets. There is no io_uring network reactor.

## Benchmarks

`./build/kv_bench --keys 1000 --ops 5000` on the development host. The table is
the unweighted mean of the six per-workload throughputs in
`benchmark_results.csv`.

| Backend | Mean ops/s | vs sync |
| --- | ---: | ---: |
| `sync` | 286k | 1.00x |
| `mmap` | 262k | 0.91x |
| `io_uring` | 217k | 0.76x |
| `threadpool` | 197k | 0.69x |

The harness sets `sync_wal = false`, so writes do not fsync the WAL. Workloads
B, D, and E use the same 95/5 uniform point-read mix; E does not scan. Treat the
table as a smoke benchmark, not a YCSB result. io_uring is fastest on read-only
workload C, at 506k against 390k for sync.

## Layout

| Path | Contents |
| --- | --- |
| `src/engine` | LSM engine: get, put, delete, flush, compaction, recovery |
| `src/wal`, `src/memtable`, `src/sstable`, `src/compaction` | Storage pieces |
| `src/io` | `IOBackend` interface and four backends |
| `src/server` | RESP2 parser, command handler, TCP server |
| `src/raft` | Standalone Raft library and TCP transport |
| `src/bench`, `tools` | Benchmark harness and chart script |
| `tests` | doctest suite |
| `docs` | Audit and figure sources |

## Tests

`ctest --test-dir build` runs 66 doctest cases with the io_uring backend enabled,
and 58 without it. The suite covers WAL replay, randomized engine operations
against `std::map`, crash recovery with SIGKILL, RESP parsing, malformed socket
input, compaction, concurrent readers, Raft persistence and snapshots, and a
three-node TCP cluster.

The crash tests use SIGKILL, which does not test power loss. The Raft tests use
sleeps and are timing-sensitive.

## Docs

- [DESIGN.md](DESIGN.md): design notes and the backend contract.
- [docs/AUDIT.md](docs/AUDIT.md): findings with severity and repros.
- [docs/figures/README.md](docs/figures/README.md): figure source and render
  command.
