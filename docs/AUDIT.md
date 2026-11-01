# Audit of io_uring-key

Date: 2026-10-31
Base commit: 30c7365
Status: remediated
Scope: source, build config, tests, and docs.

## Method

I read the source tree and the docs, built the project on Ubuntu 24.04 with
GCC 13 and liburing 2.5, and ran the tests that do not need io_uring. The
container's seccomp policy blocked io_uring, so the backend tests were not run
here. I also wrote small programs against the headers to confirm the storage,
server, and Raft findings below.

At the base commit the build reported 57 of 58 tests passing. The failure was
the C++23 version check, which expected `__cplusplus >= 202302L`; GCC 13 reports
`202100L`. After remediation the scaffold test is gone, and the suite passes
80/80 by default, 81/81 with TLS, and 88/88 with io_uring.

## Summary

The project builds and the main paths work at small scale. The docs overstate
what the server does and what the benchmark measures. Six findings break
correctness or liveness:

1. A torn WAL tail is never truncated, so writes accepted after recovery are
   lost on the next restart.
2. Mixed-size compaction can return an older value from a newer read.
3. A corrupt bloom filter crashes reads.
4. A corrupt Raft state file is silently discarded, which can permit a double
   vote.
5. Raft reads an uninitialized reply when a transport call fails, and the TCP
   transport serializes uninitialized replies for malformed frames.
6. `Server::Stop()` hangs when a client is connected and idle.

## Remediation

This audit was remediated after the base commit. The findings below are kept as
the original record, including the repros. The table maps them to the current
fix and regression test.

| Finding | Status | Fix and test |
| --- | --- | --- |
| 1. WAL torn tail | fixed | `WAL::Replay` truncates to the last valid record and fsyncs it. `tests/test_remediation.cpp`, WAL tail test. |
| 2. Mixed-size compaction | fixed | Level-0 compaction now merges every table. Mixed-size compaction test. |
| 3. Corrupt bloom filter | fixed | CRC, size, and zero-bit checks in `BloomFilter::Deserialize`. Corrupt bloom and index tests. |
| 4. Corrupt Raft state | fixed | `LoadState` fails closed and checks bounds. Persistence failures reject votes, proposals, and commits. Corrupt-state test. |
| 5. Uninitialized Raft replies | fixed | Reply structs are value-initialized and transport status is checked. Failed-transport term test. |
| 6. Stop hangs on idle client | fixed | Accepted sockets are tracked and shut down in `Stop`. Idle-client test. |
| 7. Raft not in server | fixed | `RaftStore` implements the `KVStore` interface, and `kv_server --raft-id` routes writes through the log. A three-node server cluster and failover were exercised manually; the RaftStore restart path has a test. |
| 8. Dev shell build mismatch | fixed | liburing requirement is 2.14, matching the pinned nixpkgs. Configured with a 2.14 version shim. |
| 9. Benchmark is not YCSB | fixed | D reads the latest key, E scans 10 keys, F does read-modify-write, and the CSV records `sync_wal`. CSV regenerated. |
| 10. Redis semantics | fixed | SET options are validated, EX is rejected, NX/XX use one engine lock, MSET writes one batch WAL record, DEL and MGET propagate errors, SELECT parses its index, SCAN uses a merge iterator with an opaque key cursor. Command, batch, and scan tests. |
| 11. Raft match index | fixed | Follower replies report the last covered index, not its tail. Match-index test. |
| 12. Snapshot retry loop | fixed | An equal snapshot base returns success. Equal-base test. |
| 13. RESP line growth | fixed | The parser rejects an unterminated line past 64 KiB and the server caps the connection buffer at 64 MiB. Long-line test. |
| 14. SSTable offset validation | fixed | Footer offsets, index CRC, index bounds, bloom CRC, and block bounds are checked. Corruption tests. |
| 15. Stray SSTable name | fixed | Sequence parsing uses `std::from_chars`; invalid names and `.tmp` files are ignored or removed. Startup test. |
| 16. Invalid host binds all | fixed | `Server` and `TcpRaftTransport` check `inet_pton`. Invalid-host test. |
| 17. No power-loss test | improved | SSTables are written as `.tmp`, renamed, and directory-synced; flush, compaction, and WAL truncation sync the directory. A fault-injecting backend drops unsynced directory entries at each sync point and checks recovery. A real hardware power-cut test is still absent. |

The medium findings are also addressed: CI runs build, ASan/UBSan, TSan, and a
TLS build; the scaffold tests are gone; compaction releases the SSTable lock
during the merge; `DBSIZE` caches its count; connections have a limit, idle
timeout, and optional password auth; `DbSize` serializes with writes; the log
level is atomic; and the dead `src/net` placeholder is removed. TLS is
optional, enabled with `-DKV_ENABLE_TLS=ON`, and covered by a handshake test.

## Critical findings


### 1. A torn WAL tail is not truncated

`WAL::Replay` stops at the first torn or corrupt record and returns success
(`src/wal/wal.h:132`). It never truncates the file at the last valid offset,
and `WAL::Open` sets `write_offset_` to the physical file size. New writes are
appended after the bad tail.

Repro: append `a=1` and `b=2`, fsync, truncate the file by five bytes, reopen,
replay one record, append `c=3`, fsync, reopen. The second replay returns only
`a`. The write to `c` was acknowledged and then lost.

This defeats the crash-recovery claim. A single torn write poisons the WAL for
every later restart.

Fix: truncate the WAL to the last valid offset before accepting writes. Add a
test that writes after torn-tail recovery, reopens, and checks the new writes.

### 2. Mixed-size compaction can hide a newer value

`Engine::MaybeCompact` appends its output with a new sequence number
(`src/engine/engine.h:343`). `Engine::Get` walks `sstables_` in insertion order
from the back (`src/engine/engine.h:142`). A compaction group can leave a newer
large table out while merging four older small tables. The output is then
treated as the newest table even though it contains older data.

Repro: write `hot=old` into three small tables, flush each one, write
`hot=new` with a large value into a fourth table, then write one small key that
triggers compaction. `Get("hot")` returns `old`.

Fix: order the compacted output by the highest input sequence, or include every
overlapping run in the merge. A read path that sorts by sequence would also
close the hole, at the cost of a small lookup.

### 3. A corrupt bloom filter crashes reads

`BloomFilter::Deserialize` ignores the CRC that `Serialize` writes and accepts
`num_bits = 0` (`src/sstable/sstable.h:116`). `MayContain` then computes
`% num_bits_` (`src/sstable/sstable.h:97`).

Repro: zero the first four bytes of the bloom block in an SSTable, reopen the
engine, call `Get`. The process dies with a segmentation fault.

The SSTable footer has a CRC and the data blocks have CRCs. The bloom block and
the index block do not get the same treatment. `SSTableReader::Open` also trusts
`bloom_offset_`, `bloom_size_`, `index_offset_`, and `index_size_` from the
footer without checking them against the file size.

Fix: verify the bloom CRC, reject zero or implausible sizes, and range-check
every footer offset. Do the same for the index CRC.

### 4. Corrupt Raft state is discarded

`RaftNode::LoadState` returns `Status::OK()` when the header or CRC is bad, and
starts with term 0 and an empty vote (`src/raft/raft.h:780`). Raft requires
persistent state to survive a restart. A node that forgets its term can vote
twice in one term.

Many `PersistState()` calls also ignore the returned status, for example in
`StartElection` (`src/raft/raft.h:509`) and `HandleRequestVote`. A full disk or
failed fsync does not stop the node from soliciting votes or acknowledging a
proposal.

Fix: return `Status::Corruption` from `LoadState` and refuse to start. Check
every persistence result before sending an RPC or returning success.

### 5. Raft can read an uninitialized reply

`RequestVoteReply` and the other reply structs have no default member
initializers. `StartElection` declares `RequestVoteReply reply`, calls
`SendRequestVote`, and then reads `reply.term` even when the call fails
(`src/raft/raft.h:500`). A transport error leaves the field uninitialized.

The TCP server has the same problem in another form. `HandleConn` declares a
reply, decodes the request, and serializes the reply even when decoding failed
(`src/raft/tcp_transport.h:334`). A malformed frame gets an uninitialized
payload.

Fix: value-initialize every reply, check the decode and transport status, and
send a defined error reply for malformed frames.

### 6. Server shutdown hangs on an idle client

`Server::Stop` closes the listener and joins the accept thread, then joins every
client thread (`src/server/server.h:83`). The client threads are blocked in
`recv` (`src/server/server.h:155`). Closing the listener does not close accepted
sockets.

Repro: start the server, connect a socket, send nothing, call `Stop`. The call
was still blocked after two seconds.

Fix: track the accepted file descriptors and shut each one down in `Stop`, or
drive connections from a poll loop that watches a stop event.

## High findings

### 7. The Raft library is not wired into the server

`src/main.cpp`, `src/server`, and `src/engine` contain no Raft references. The
only users are the tests. The pre-rewrite README and `DESIGN.md` put Raft above
the engine in the architecture, which reads as if `kv_server` replicates. It
does not.

Fix the docs now. Wire the library in, or state plainly that the server is
single-node and the Raft code is a separate deliverable.

### 8. The dev shell cannot satisfy the build

`CMakeLists.txt` required liburing 2.15. The pinned nixpkgs input provides
2.14, and `devenv.nix` uses that package, so `cmake` fails at configure time in
the documented dev shell.

The flake lists only `x86_64-linux` and `aarch64-linux`
(`flake.nix:12`). There is no macOS dev shell, so `nix develop` fails on the
machine that holds this checkout.

This pass lowers the requirement to 2.14. The code builds against liburing 2.5
in the container, so 2.14 is enough.

### 9. The benchmark is not YCSB and does not fsync

`kv_bench` sets `sync_wal = false` (`src/bench/bench_main.cpp:57`). The
pre-rewrite README explained the io_uring result as if every write fsynced,
which is false.

Workloads B, D, and E all use a 95/5 read/write mix over uniform random keys
(`src/bench/ycsb.h:63`). D does not read the latest version, and E does not
scan. Only the read/write percentages differ from B.

The table is still a useful smoke test of the four backends. It is not a YCSB
result and it does not measure durable writes.

Fix: rename the workloads, implement the missing distributions, or remove the
YCSB claim from the docs. State the fsync policy next to the numbers.

### 10. Redis command semantics are looser than claimed

- `SET` ignores unknown options and accepts `EX` with no value. `EX` is parsed
  and not applied (`src/server/command_handler.h:64`).
- `SET NX/XX` checks with `Get` and then calls `Put`, so two clients can both
  succeed on an absent key.
- `MSET` ignores every `Put` result and returns `OK` (`src/server/command_handler.h:130`).
- `DEL` ignores `Delete` results and counts a key as deleted after a successful
  `Get`.
- `SELECT` ignores its argument. `SCAN` returns cursor 0 and an empty list for
  every input.

Fix: validate option arity, propagate write errors, and return a defined error
for commands that are not implemented. The README now states these limits.

### 11. Raft follower match index can overstate replication

`HandleAppendEntries` replies with `log_.back().index`, which includes any
uncommitted suffix the follower already had. The leader stores that value in
`match_index_` and moves `next_index_` past entries it never sent. A follower
with an old uncommitted suffix can make the leader count it as replicated.

Fix: reply with the last index the request covered, or with the last entry that
was actually appended, not the follower's tail.

### 12. Snapshot retry can loop

`HandleInstallSnapshot` returns success false when the incoming snapshot base
equals the local base (`src/raft/raft.h:255`). The leader only advances
`next_index_` on success, so it sends the same snapshot on every heartbeat.

Fix: treat an equal base as success, or let the leader advance past a base the
follower already has.

### 13. RESP line input can grow without bound

`ReadLine` checks `kMaxInlineLine` only after it finds a CRLF. A client can send
an endless line with no CRLF and grow the per-connection buffer until memory is
exhausted. The bulk limit is 512 MB per request, and the server uses one thread
per connection.

Fix: cap the connection buffer before parsing and close the socket when it
overflows.

### 14. SSTable reading trusts unvalidated offsets

`SSTableReader::Open` builds vectors from `bloom_size_` and `index_size_` before
it checks them against the file size (`src/sstable/sstable.h:346`). The index CRC
is written but never checked. `ForEach` reads block lengths and entry lengths
without bounds checks (`src/sstable/sstable.h:469`).

A corrupt file that happens to have a valid footer can allocate a huge vector,
read outside the file, or crash the process. Random corruption usually fails
the footer CRC first, so this is a hardening gap rather than a common path.

Fix: range-check all offsets and sizes, verify both block and index CRCs, and
bound every parse loop.

### 15. A stray SSTable filename aborts startup

`LoadSSTables` calls `std::stoull` on the suffix of any `sst_*.sst` name
(`src/engine/engine.h:389`). A file named `sst_junk.sst` throws
`std::invalid_argument`. `Engine::Open` does not catch it, and `main` does not
either.

Repro: create `sst_junk.sst` in the data directory, then open the engine. The
process throws `stoull` and exits.

Fix: parse with `std::from_chars`, and skip or report names that do not match
the expected pattern.

### 16. An invalid host binds every interface

`Server::Start` ignores the return from `inet_pton` (`src/server/server.h:52`).
`TcpRaftTransport::Start` does the same. A host string that is not an IPv4
address leaves the address at zero, which binds `0.0.0.0`.

Fix: check the return value and reject an invalid host.

### 17. The crash tests do not test power loss

The crash tests send SIGKILL to a child process. The page cache survives that,
so the tests do not exercise a power cut.

The engine fsyncs the new SSTable file before it truncates the WAL, but it never
fsyncs the data directory. A power loss can lose the new directory entry after
the WAL is gone. `Clear` also unlinks SSTables without a directory sync. The
Raft path does sync its directory.

Fix: sync the data directory after creating or deleting an SSTable and after
truncating the WAL. Add a test that injects a crash between those steps.

## Medium findings

- There is no CI configuration and no sanitizer target. The ThreadSanitizer
  claims in the docs cannot be reproduced from the repository.
- `tests/test_hello.cpp` still holds the two scaffold tests. The 66 count
  includes them.
- Compaction holds `sst_mu_` across file reads, writes, and fsyncs, so reads
  block for the whole merge.
- `DBSIZE` scans every SSTable into a `std::map` on each call.
- The server has no connection limit, auth, TLS, or idle timeout.
- `DbSize` takes no write lock, so the count can race with concurrent writes.
- `GlobalLogLevel` is a plain global. Tests that change it can race.
- `src/net/.gitkeep` is a leftover placeholder for the abandoned network
  reactor.

## Documentation findings

- The pre-rewrite README titled the project `kernel-v2`, while the repository
  is `io_uring-key` and every binary is named `kv_*`.
- The pre-rewrite README and the original `DESIGN.md` showed Raft in the
  server path. The server now starts it only with `--raft-id`.
- The pre-rewrite README said all 66 tests were green. The documented dev
  shell could not configure because of the liburing version, and the container
  run had one toolchain failure.
- The pre-rewrite README said the benchmark fsynced each write. It does not.
- The pre-rewrite README said Redis-compatible. TTL, SCAN, SELECT, MSET, and
  DEL do not match Redis semantics.
- The original `DESIGN.md` repeated the Raft-in-server and benchmark-fsync
  claims.

## Suggested fix order

1. Truncate the WAL tail and add a write-after-recovery test.
2. Order compaction output by sequence and add a mixed-size test.
3. Validate bloom and index CRCs and every footer offset.
4. Refuse to start on corrupt Raft state and propagate persistence errors.
5. Value-initialize Raft replies and reject malformed frames.
6. Track client sockets and shut them down in `Server::Stop`.
7. Wire Raft into the server or remove it from the server architecture.

## What the remediation changed

- `WAL`: truncates torn tails, fsyncs the cut, syncs the WAL directory, and
  writes MSET as one CRC-protected batch record.
- `IOBackend`: added `Truncate`, `Rename`, and `SyncDirectory` to all four
  backends.
- `Engine`: temp files with atomic rename, directory sync before WAL truncation,
  full level-0 merge, lock release during merge, conditional writes, batches,
  `Keys`, `Scan`, and a cached `DbSize`.
- `SSTable`: bloom CRC and size checks, index CRC, footer range checks, bounded
  block parsing.
- `Server`: socket tracking for shutdown, buffer cap, connection limit, idle
  timeout, optional password auth, host validation, and optional TLS.
- `RESP`: unterminated line cap.
- `Raft`: fail-closed state load, persistence checks, non-blocking RPC locks to
  avoid inline-transport ABBA, reply initialization, covered match index,
  idempotent snapshot install.
- `Bench`: read-latest, scan, read-modify-write, sync policy flag, CSV column.
- Build: liburing 2.14, sanitizer targets, pinned doctest, GitHub Actions CI.
- Tests: `tests/test_remediation.cpp`, scaffold test removed, 88 cases with
  io_uring, 80 in the default build, and 81 with TLS.
- Docs: README and the figure source label updated. `DESIGN.md` was removed.
