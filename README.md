<h1 align="center" style="letter-spacing: 0.12em;">io_uring-key</h1>

<p align="center">A C++23 log-structured key-value store with a RESP2 server, four I/O backends, and optional Raft mode.</p>

<br>

## What this is

`kv_server` is a single-process key-value server. It speaks RESP2 over TCP and
stores data in an LSM tree: a write-ahead log, a skip-list memtable, block-based
SSTables, and level-0 full compaction. Four backends implement one I/O
interface: `sync`, `threadpool`, `mmap`, and `io_uring`.

The repository also contains a Raft consensus library, a YCSB-inspired benchmark
harness, and a chart script. `kv_server` can run standalone or as a Raft node.
The source and runtime messages use the internal name `kernel-v2`.

This is a research prototype for systems programmers. It has known gaps. Read
[Limits](#limits) before you trust it with data.

## Requirements

The dev shell supplies liburing 2.14, GCC 16, Clang 22, CMake 4.1, Python 3,
matplotlib, and OpenSSL. Without Nix you need Linux with io_uring, liburing 2.14
or newer, CMake 3.28 or newer, and a C++23 compiler. The CI build uses GCC 13.
OpenSSL is needed only for `-DKV_ENABLE_TLS=ON`.

## Quick start

Linux only. The flake has dev shells for `x86_64-linux` and `aarch64-linux`;
there is no macOS dev shell.

```bash
nix develop
cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build -j
ctest --test-dir build --output-on-failure
```

For TLS, configure with `-DKV_ENABLE_TLS=ON` and pass `--tls-cert` and
`--tls-key` when starting the server.

For a single-node Raft server, add `--raft-id` and `--raft-port`:

```bash
./build/kv_server --port 6379 --backend sync --data-dir /tmp/kv_raft \
  --raft-id n1 --raft-host 127.0.0.1 --raft-port 7001
```

Add one `--raft-peer id@host:port` per node, including self, for a cluster.

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

Pass `--sync-wal` to `kv_bench` when you want each write fsynced. The default
skips the WAL fsync and records that policy in the CSV.

## Architecture

![Architecture](docs/figures/architecture.png)

`kv_server` reads and writes through the LSM engine and one I/O backend. In
Raft mode, writes commit through the Raft log first and the committed entries
are applied to the engine. The figure source and render command are in
[docs/figures/README.md](docs/figures/README.md).

## What works

- LSM engine: CRC-checked WAL with torn-tail repair, skip-list memtable,
  block SSTables with sparse indexes and bloom filters, level-0 full
  compaction, and temp-file renames with directory sync.
- I/O backends: `sync`, `threadpool`, `mmap`, and `io_uring`. The io_uring
  backend supports batched submission, registered buffers and files, and
  optional SQPOLL with a fallback. The engine still submits one operation at a
  time on its hot path.
- RESP2 server: `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`, `MGET`, `MSET`,
  `DBSIZE`, `FLUSHDB`, `INFO`, `SELECT`, `COMMAND`, `SCAN`, and `AUTH`.
- Transport: optional TLS with OpenSSL, enabled at build time with
  `-DKV_ENABLE_TLS=ON` and at runtime with `--tls-cert` and `--tls-key`.
- Server controls: optional shared-password auth, a connection limit, and an
  idle timeout. Set `--requirepass secret` or `KV_REQUIREPASS` to enable auth.
- Raft mode: `--raft-id` starts a Raft node, routes writes through the log,
  and applies committed entries to the engine. Elections, replication,
  snapshots, and the TCP transport are covered by tests.
- Benchmarks: YCSB-inspired A to F, including read-latest (D), 10-key scans
  (E), and read-modify-write (F). CSV export and matplotlib charts.

## Limits

- `SET ... EX` is rejected with an error. TTL is not implemented.
- `MSET` is one batch WAL record. A torn batch is discarded as a unit, so it
  does not leave a prefix. A crash after a full batch is recovered as a batch.
- In Raft mode, writes are leader-only and reads are leader-only. Followers
  reject commands with `NotSupported: not the leader`; there is no redirect.
- Raft mode rejects `SET NX/XX`, `MSET`, and `FLUSHDB`. Reads use a leader
  lease, not a ReadIndex confirmation.
- A Raft commit timeout returns an error, but the entry can still commit later.
- The Raft tests use sleeps and are timing-sensitive.
- TLS is optional. Without `--tls-cert` and `--tls-key`, `AUTH` is a shared
  password over plain TCP, so do not expose that mode to an untrusted network.
- `SCAN` uses an opaque hex key as its cursor, not a numeric Redis cursor.
  Writes between pages can appear or disappear.
- The benchmark uses uniform keys, not the Zipfian distribution of the default
  YCSB driver.
- Crash tests use SIGKILL. Directory fsync is covered by a fault-injection test,
  not by a real power cut.
- Compaction is a full level-0 merge. There is no leveled compaction.

## Benchmarks

`./build/kv_bench --keys 1000 --ops 5000` on a Linux 7.2 aarch64 VM with four
vCPUs. All rows use `sync_wal=false`.

| Backend | A | B | C | D | E | F |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `sync` | 1.00M | 3.21M | 5.93M | 3.86M | 47.2k | 907.5k |
| `threadpool` | 71.9k | 591.1k | 5.77M | 642.2k | 38.2k | 70.9k |
| `mmap` | 46.0k | 406.2k | 5.70M | 481.3k | 37.5k | 48.2k |
| `io_uring` | 92.6k | 722.4k | 5.50M | 795.9k | 37.6k | 88.6k |

A is 50/50 read/write, B is 95/5, C is read-only, D reads the latest key, E
spends 95% of operations on 10-key scans, and F spends 50% on read-modify-write.
The scan path merges the memtable and SSTables one block at a time, so E stays
slower than point reads but no longer rebuilds the full key set. Treat the table
as a smoke benchmark, not a production comparison.

## Tests

`ctest --test-dir build` runs 80 doctest cases in the default build, 81 with
TLS, and 88 with the io_uring backend. The suite covers WAL tail repair, batch
WAL recovery, randomized engine operations against `std::map`, SIGKILL
recovery, simulated power loss, directory fsync ordering, RESP parsing,
malformed socket input, merge scans, compaction, concurrent readers, Raft
persistence and snapshots, a three-node TCP cluster, and a TLS handshake.

CI runs the default 80-case build, AddressSanitizer with UndefinedBehaviorSanitizer,
ThreadSanitizer, and a TLS build. Run the 88-case io_uring suite on a Linux host.

The crash tests use SIGKILL, and the power-loss test uses a fault-injecting
backend that drops unsynced directory entries. Neither is a real hardware power
cut.

## Docs

- [docs/AUDIT.md](docs/AUDIT.md): the original findings and their remediation.
- [docs/figures/README.md](docs/figures/README.md): figure source and render
  command.
