#include <doctest/doctest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "engine/engine.h"
#include "io/mmap_backend.h"
#include "io/sync_backend.h"
#include "io/threadpool_backend.h"

#ifdef KV_HAS_IOURING
#include "io/iouring_backend.h"
#endif
#include "raft/raft.h"
#include "server/command_handler.h"
#include "server/server.h"
#include "sstable/sstable.h"
#include "util/crc32.h"
#include "wal/wal.h"

#ifdef KV_HAS_TLS
#include <openssl/ssl.h>
#endif

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_remediation_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

std::string ReadFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(f)),
                       std::istreambuf_iterator<char>());
}

void WriteFile(const std::string& path, const std::string& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f.write(data.data(), static_cast<std::streamsize>(data.size()));
}

void FlipByte(const std::string& path, size_t offset) {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    REQUIRE(f.is_open());
    f.seekg(static_cast<std::streamoff>(offset));
    char value = 0;
    f.read(&value, 1);
    value = static_cast<char>(value ^ 0x5A);
    f.seekp(static_cast<std::streamoff>(offset));
    f.write(&value, 1);
}

struct FooterLayout {
    uint64_t index_offset = 0;
    uint32_t index_size = 0;
    uint64_t bloom_offset = 0;
    uint32_t bloom_size = 0;
    uint64_t data_size = 0;
};

FooterLayout ReadFooter(const std::string& path) {
    auto data = ReadFile(path);
    REQUIRE(data.size() >= kFooterSize);
    const char* footer = data.data() + data.size() - kFooterSize;
    FooterLayout layout;
    std::memcpy(&layout.index_offset, footer + 0, 8);
    std::memcpy(&layout.index_size, footer + 8, 4);
    std::memcpy(&layout.bloom_offset, footer + 12, 8);
    std::memcpy(&layout.bloom_size, footer + 20, 4);
    std::memcpy(&layout.data_size, footer + 24, 8);
    return layout;
}

void PatchFooterCrc(const std::string& path) {
    auto data = ReadFile(path);
    REQUIRE(data.size() >= kFooterSize);
    size_t start = data.size() - kFooterSize;
    uint32_t crc = CRC32::Compute(data.data() + start, 40);
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    REQUIRE(f.is_open());
    f.seekp(static_cast<std::streamoff>(start + 40));
    f.write(reinterpret_cast<const char*>(&crc), 4);
}

void PatchBloomCrc(const std::string& path) {
    auto layout = ReadFooter(path);
    auto data = ReadFile(path);
    REQUIRE(layout.bloom_offset + layout.bloom_size <= data.size());
    uint32_t crc = CRC32::Compute(data.data() + layout.bloom_offset,
                                  layout.bloom_size - 4);
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    REQUIRE(f.is_open());
    f.seekp(static_cast<std::streamoff>(layout.bloom_offset +
                                        layout.bloom_size - 4));
    f.write(reinterpret_cast<const char*>(&crc), 4);
}

std::string CreateSstable(const std::string& dir) {
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = true;
    {
        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());
        REQUIRE(engine.Put("alpha", "one").ok());
        REQUIRE(engine.FlushAndSync().ok());
    }
    for (auto& entry : std::filesystem::directory_iterator(dir)) {
        auto name = entry.path().filename().string();
        if (name.starts_with("sst_") && name.ends_with(".sst")) {
            return entry.path().string();
        }
    }
    REQUIRE(false);
    return {};
}

class RecordingBackend final : public IOBackend {
public:
    [[nodiscard]] std::string_view Name() const override { return "recording"; }

    Status Open(std::string_view path, bool create, bool direct,
                FileHandle* out) override {
        return inner_.Open(path, create, direct, out);
    }
    Status Close(FileHandle fh) override { return inner_.Close(fh); }
    Status Delete(std::string_view path) override { return inner_.Delete(path); }
    Status Rename(std::string_view from, std::string_view to) override {
        return inner_.Rename(from, to);
    }
    Status FileSize(FileHandle fh, uint64_t* size) override {
        return inner_.FileSize(fh, size);
    }
    Status Truncate(FileHandle fh, uint64_t size) override {
        return inner_.Truncate(fh, size);
    }
    Status SyncDirectory(std::string_view path) override {
        sync_dir_calls_.fetch_add(1);
        if (fail_dir_sync_.load()) {
            return Status::IOError("injected directory sync failure");
        }
        return inner_.SyncDirectory(path);
    }
    Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) override {
        return inner_.Read(fh, offset, buf, len);
    }
    Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) override {
        return inner_.Write(fh, offset, buf, len);
    }
    Status Append(FileHandle fh, const void* buf, size_t len,
                  uint64_t* offset) override {
        return inner_.Append(fh, buf, len, offset);
    }
    Status Sync(FileHandle fh, bool data_only) override {
        return inner_.Sync(fh, data_only);
    }

    void set_fail_dir_sync(bool fail) { fail_dir_sync_.store(fail); }
    [[nodiscard]] int sync_dir_calls() const { return sync_dir_calls_.load(); }

private:
    SyncBackend inner_;
    std::atomic<int> sync_dir_calls_{0};
    std::atomic<bool> fail_dir_sync_{false};
};

class CrashBackend final : public IOBackend {
public:
    [[nodiscard]] std::string_view Name() const override { return "crash"; }

    void arm(int syncs) { crash_at_ = sync_calls_ + syncs; }
    [[nodiscard]] bool crashed() const { return crashed_; }

    Status Open(std::string_view path, bool create, bool direct,
                FileHandle* out) override {
        std::string owned(path);
        bool existed = std::filesystem::exists(owned);
        auto s = inner_.Open(path, create, direct, out);
        if (s.ok() && create && !existed) pending_creates_.insert(owned);
        return s;
    }
    Status Close(FileHandle fh) override { return inner_.Close(fh); }
    Status Delete(std::string_view path) override {
        std::string owned(path);
        if (std::filesystem::exists(owned)) {
            pending_deletes_[owned] = ReadFile(owned);
        }
        return inner_.Delete(path);
    }
    Status Rename(std::string_view from, std::string_view to) override {
        std::string from_str(from);
        std::string to_str(to);
        auto s = inner_.Rename(from, to);
        if (s.ok()) pending_renames_.emplace_back(from_str, to_str);
        return s;
    }
    Status FileSize(FileHandle fh, uint64_t* size) override {
        return inner_.FileSize(fh, size);
    }
    Status Truncate(FileHandle fh, uint64_t size) override {
        return inner_.Truncate(fh, size);
    }
    Status SyncDirectory(std::string_view path) override {
        sync_calls_++;
        if (crash_at_ >= 0 && sync_calls_ >= crash_at_) {
            Crash();
            return Status::IOError("simulated power loss");
        }
        ClearPending();
        return inner_.SyncDirectory(path);
    }
    Status Read(FileHandle fh, uint64_t offset, void* buf, size_t len) override {
        return inner_.Read(fh, offset, buf, len);
    }
    Status Write(FileHandle fh, uint64_t offset, const void* buf, size_t len) override {
        return inner_.Write(fh, offset, buf, len);
    }
    Status Append(FileHandle fh, const void* buf, size_t len,
                  uint64_t* offset) override {
        return inner_.Append(fh, buf, len, offset);
    }
    Status Sync(FileHandle fh, bool data_only) override {
        return inner_.Sync(fh, data_only);
    }

private:
    void ClearPending() {
        pending_creates_.clear();
        pending_renames_.clear();
        pending_deletes_.clear();
    }

    void Crash() {
        if (crashed_) return;
        crashed_ = true;
        crash_at_ = -1;

        for (auto& [path, data] : pending_deletes_) {
            WriteFile(path, data);
            pending_creates_.erase(path);
        }
        for (auto it = pending_renames_.rbegin(); it != pending_renames_.rend(); ++it) {
            if (std::filesystem::exists(it->second) &&
                !std::filesystem::exists(it->first)) {
                std::filesystem::rename(it->second, it->first);
            }
        }
        for (auto& path : pending_creates_) {
            std::filesystem::remove(path);
        }
        ClearPending();
    }

    SyncBackend inner_;
    std::set<std::string> pending_creates_;
    std::vector<std::pair<std::string, std::string>> pending_renames_;
    std::map<std::string, std::string> pending_deletes_;
    int sync_calls_ = 0;
    int crash_at_ = -1;
    bool crashed_ = false;
};

class FailingTransport final : public RaftTransport {
public:
    Status SendRequestVote(const std::string&, const RequestVoteArgs&,
                           RequestVoteReply* reply) override {
        reply->term = 999;
        reply->vote_granted = true;
        return Status::IOError("injected vote failure");
    }
    Status SendAppendEntries(const std::string&, const AppendEntriesArgs&,
                             AppendEntriesReply* reply) override {
        reply->term = 999;
        reply->success = true;
        return Status::IOError("injected append failure");
    }
};

int Connect(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    REQUIRE(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
    REQUIRE(::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0);
    return fd;
}

void SendAll(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t w = ::send(fd, data.data() + sent, data.size() - sent, 0);
        REQUIRE(w > 0);
        sent += static_cast<size_t>(w);
    }
}

std::string RecvUntilQuiet(int fd, int quiet_ms = 200) {
    std::string out;
    char buf[4096];
    struct timeval tv{};
    tv.tv_sec = 0;
    tv.tv_usec = quiet_ms * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    while (true) {
        ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0) break;
        out.append(buf, static_cast<size_t>(n));
    }
    return out;
}

std::string BulkCommand(const std::vector<std::string>& args) {
    std::string out = "*" + std::to_string(args.size()) + "\r\n";
    for (auto& arg : args) {
        out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    }
    return out;
}

std::vector<std::string> RespStrings(const RespValue& value) {
    std::vector<std::string> out;
    for (auto& item : value.array) {
        if (item.type == RespType::kBulkString && !item.is_null) {
            out.push_back(item.str);
        }
    }
    return out;
}

} // namespace

TEST_CASE("WAL: a torn tail is truncated and later writes survive") {
    auto dir = MakeTempDir();
    SyncBackend backend;

    {
        WAL wal(&backend, dir);
        REQUIRE(wal.Open().ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "a", "1"}).ok());
        REQUIRE(wal.Append({WALRecordType::kPut, "b", "2"}).ok());
        REQUIRE(wal.Sync().ok());
    }

    auto path = dir + "/wal.log";
    auto size = std::filesystem::file_size(path);
    REQUIRE(size > 10);
    std::filesystem::resize_file(path, size - 5);

    {
        WAL wal(&backend, dir);
        REQUIRE(wal.Open().ok());
        size_t replayed = 0;
        REQUIRE(wal.Replay([&](const WALRecord&) { return true; }, &replayed).ok());
        CHECK(replayed == 1);
        REQUIRE(wal.Append({WALRecordType::kPut, "c", "3"}).ok());
        REQUIRE(wal.Sync().ok());
    }

    {
        WAL wal(&backend, dir);
        REQUIRE(wal.Open().ok());
        std::vector<std::string> keys;
        REQUIRE(wal.Replay([&](const WALRecord& rec) {
            keys.push_back(rec.key);
            return true;
        }).ok());
        REQUIRE(keys.size() == 2);
        CHECK(keys[0] == "a");
        CHECK(keys[1] == "c");
    }

    std::filesystem::remove_all(dir);
}

TEST_CASE("compaction: mixed-size tables keep the newest value") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = false;

    {
        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());

        for (int i = 0; i < 3; i++) {
            REQUIRE(engine.Put("hot", "old").ok());
            REQUIRE(engine.FlushMemtable().ok());
        }

        REQUIRE(engine.Put("hot", std::string(5000, 'n')).ok());
        REQUIRE(engine.FlushMemtable().ok());

        CHECK(engine.sstable_count() == 1);
        auto value = engine.Get("hot");
        REQUIRE(value.has_value());
        CHECK(value->size() == 5000);
        CHECK((*value)[0] == 'n');
    }

    auto reopen_backend = std::make_unique<SyncBackend>();
    Engine reopened(std::move(reopen_backend), opts);
    REQUIRE(reopened.Open().ok());
    auto value = reopened.Get("hot");
    REQUIRE(value.has_value());
    CHECK(value->size() == 5000);

    std::filesystem::remove_all(dir);
}

TEST_CASE("sstable: corrupt bloom, index, and footer are rejected") {
    auto dir = MakeTempDir();
    auto path = CreateSstable(dir);
    auto layout = ReadFooter(path);

    SUBCASE("bloom CRC mismatch") {
        FlipByte(path, static_cast<size_t>(layout.bloom_offset + 8));
        SyncBackend backend;
        SSTableReader reader(&backend, path);
        auto s = reader.Open();
        CHECK(s.IsCorruption());
    }

    SUBCASE("zero bloom bit count") {
        auto data = ReadFile(path);
        uint32_t zero = 0;
        std::memcpy(data.data() + layout.bloom_offset, &zero, 4);
        WriteFile(path, data);
        PatchBloomCrc(path);

        SyncBackend backend;
        SSTableReader reader(&backend, path);
        auto s = reader.Open();
        CHECK(s.IsCorruption());
    }

    SUBCASE("index CRC mismatch") {
        FlipByte(path, static_cast<size_t>(layout.index_offset + 4));
        SyncBackend backend;
        SSTableReader reader(&backend, path);
        auto s = reader.Open();
        CHECK(s.IsCorruption());
    }

    SUBCASE("footer offset outside the file") {
        auto data = ReadFile(path);
        size_t start = data.size() - kFooterSize;
        uint64_t bad_offset = data.size() * 2;
        std::memcpy(data.data() + start + 12, &bad_offset, 8);
        WriteFile(path, data);
        PatchFooterCrc(path);

        SyncBackend backend;
        SSTableReader reader(&backend, path);
        auto s = reader.Open();
        CHECK(s.IsCorruption());
    }

    std::filesystem::remove_all(dir);
}

TEST_CASE("engine: stray and temporary SSTable names do not abort startup") {
    auto dir = MakeTempDir();
    WriteFile(dir + "/sst_junk.sst", "not a table");
    WriteFile(dir + "/sst_0.sst.tmp", "partial table");

    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    Engine engine(std::move(backend), opts);
    CHECK(engine.Open().ok());
    CHECK(!std::filesystem::exists(dir + "/sst_0.sst.tmp"));

    std::filesystem::remove_all(dir);
}

TEST_CASE("engine: flush makes the directory entry durable before truncating WAL") {
    auto dir = MakeTempDir();
    auto recording = std::make_unique<RecordingBackend>();
    auto* raw = recording.get();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = false;

    Engine engine(std::move(recording), opts);
    REQUIRE(engine.Open().ok());
    REQUIRE(engine.Put("key", "value").ok());
    REQUIRE(engine.FlushMemtable().ok());
    CHECK(raw->sync_dir_calls() > 0);
    CHECK(std::filesystem::file_size(dir + "/wal.log") == 0);

    std::filesystem::remove_all(dir);
}

TEST_CASE("engine: a failed directory sync leaves the WAL intact") {
    auto dir = MakeTempDir();
    auto recording = std::make_unique<RecordingBackend>();
    auto* raw = recording.get();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = true;

    {
        Engine engine(std::move(recording), opts);
        REQUIRE(engine.Open().ok());
        REQUIRE(engine.Put("key", "value").ok());
        raw->set_fail_dir_sync(true);
        auto s = engine.FlushMemtable();
        CHECK(!s.ok());
        CHECK(std::filesystem::file_size(dir + "/wal.log") > 0);

        raw->set_fail_dir_sync(false);
        REQUIRE(engine.FlushMemtable().ok());
    }

    auto backend = std::make_unique<SyncBackend>();
    Engine reopened(std::move(backend), opts);
    REQUIRE(reopened.Open().ok());
    auto value = reopened.Get("key");
    REQUIRE(value.has_value());
    CHECK(*value == "value");

    std::filesystem::remove_all(dir);
}

TEST_CASE("raft: corrupt persisted state fails closed") {
    auto dir = MakeTempDir();
    WriteFile(dir + "/raft_state.bin", "not valid raft state");

    InMemoryTransport transport;
    RaftConfig config;
    config.node_id = "n1";
    config.peers = {"n1"};
    config.data_dir = dir;

    RaftNode node(config, &transport, nullptr);
    auto s = node.Start();
    CHECK(s.IsCorruption());
    node.Stop();

    std::filesystem::remove_all(dir);
}

TEST_CASE("raft: a failed transport cannot inject a term") {
    FailingTransport transport;
    RaftConfig config;
    config.node_id = "n1";
    config.peers = {"n1", "n2", "n3"};
    config.election_timeout_min_ms = 10;
    config.election_timeout_max_ms = 20;
    config.heartbeat_interval_ms = 10;

    RaftNode node(config, &transport, nullptr);
    REQUIRE(node.Start().ok());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK(node.current_term() < 999);
    node.Stop();
}

TEST_CASE("raft: append reply reports the covered index, not the follower tail") {
    auto dir = MakeTempDir();
    InMemoryTransport transport;
    RaftConfig config;
    config.node_id = "n1";
    config.peers = {"n1"};
    config.data_dir = dir;

    RaftNode node(config, &transport, nullptr);

    AppendEntriesArgs first{};
    first.term = 1;
    first.leader_id = "n2";
    first.prev_log_index = 0;
    first.prev_log_term = 0;
    first.entries = {
        LogEntry{1, 1, "a", "1", false},
        LogEntry{1, 2, "b", "2", false},
    };
    auto first_reply = node.HandleAppendEntries(first);
    REQUIRE(first_reply.success);
    CHECK(first_reply.match_index == 2);

    AppendEntriesArgs heartbeat{};
    heartbeat.term = 1;
    heartbeat.leader_id = "n2";
    heartbeat.prev_log_index = 1;
    heartbeat.prev_log_term = 1;
    auto second_reply = node.HandleAppendEntries(heartbeat);
    REQUIRE(second_reply.success);
    CHECK(second_reply.match_index == 1);

    std::filesystem::remove_all(dir);
}

TEST_CASE("raft: an equal snapshot base succeeds without rolling state back") {
    auto dir = MakeTempDir();
    InMemoryTransport transport;
    RaftConfig config;
    config.node_id = "n1";
    config.peers = {"n1"};
    config.data_dir = dir;

    RaftNode node(config, &transport, nullptr);

    InstallSnapshotArgs first{};
    first.term = 5;
    first.leader_id = "n2";
    first.last_included_index = 100;
    first.last_included_term = 4;
    first.data = {{"a", "1"}};
    REQUIRE(node.HandleInstallSnapshot(first).success);

    InstallSnapshotArgs equal{};
    equal.term = 6;
    equal.leader_id = "n2";
    equal.last_included_index = 100;
    equal.last_included_term = 4;
    equal.data = {{"a", "stale"}};
    auto reply = node.HandleInstallSnapshot(equal);
    CHECK(reply.success);
    CHECK(node.state_machine_copy()["a"] == "1");

    std::filesystem::remove_all(dir);
}

TEST_CASE("commands: SET options, MSET, DEL, SELECT, and SCAN are defined") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = false;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());
    CommandHandler handler(engine);

    auto unknown = handler.Execute({"SET", "k", "v", "BOGUS"});
    CHECK(unknown.type == RespType::kError);

    auto ttl = handler.Execute({"SET", "k", "v", "EX", "10"});
    CHECK(ttl.type == RespType::kError);
    CHECK(handler.Execute({"GET", "k"}).is_null);

    auto nx = handler.Execute({"SET", "k", "v", "NX"});
    CHECK(nx.type == RespType::kSimpleString);
    auto nx_again = handler.Execute({"SET", "k", "v2", "NX"});
    CHECK(nx_again.is_null);

    auto xx = handler.Execute({"SET", "k", "v3", "XX"});
    CHECK(xx.type == RespType::kSimpleString);
    auto xx_missing = handler.Execute({"SET", "missing", "v", "XX"});
    CHECK(xx_missing.is_null);
    CHECK(handler.Execute({"GET", "k"}).str == "v3");

    auto mset = handler.Execute({"MSET", "a", "1", "b", "2"});
    CHECK(mset.type == RespType::kSimpleString);
    auto mget = handler.Execute({"MGET", "a", "b", "missing"});
    REQUIRE(mget.array.size() == 3);
    CHECK(mget.array[0].str == "1");
    CHECK(mget.array[1].str == "2");
    CHECK(mget.array[2].is_null);

    auto dbsize = handler.Execute({"DBSIZE"});
    CHECK(dbsize.integer == 3);

    auto deleted = handler.Execute({"DEL", "a", "b"});
    CHECK(deleted.integer == 2);
    CHECK(handler.Execute({"DBSIZE"}).integer == 1);

    CHECK(handler.Execute({"SELECT", "1"}).type == RespType::kError);
    CHECK(handler.Execute({"SELECT", "0"}).type == RespType::kSimpleString);
    CHECK(handler.Execute({"PING", "a", "b"}).type == RespType::kError);
    CHECK(handler.Execute({"AUTH", "x"}).type == RespType::kError);

    auto scan = handler.Execute({"SCAN", "0", "COUNT", "10"});
    REQUIRE(scan.array.size() == 2);
    CHECK(scan.array[0].str == "0");
    auto keys = RespStrings(scan.array[1]);
    REQUIRE(keys.size() == 1);
    CHECK(keys[0] == "k");

    auto match = handler.Execute({"SCAN", "0", "MATCH", "z*"});
    REQUIRE(match.array.size() == 2);
    CHECK(RespStrings(match.array[1]).empty());

    std::filesystem::remove_all(dir);
}

TEST_CASE("RESP: an unterminated long line is rejected without waiting") {
    std::string input = "+" + std::string(70000, 'x');
    auto parsed = RespParser::ParseCommandSafe(input);
    CHECK(parsed.status == ParseStatus::kError);
}

TEST_CASE("server: Stop unblocks an idle client") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    ServerOptions server_opts;
    server_opts.port = 0;
    Server server(engine, server_opts);
    REQUIRE(server.Start().ok());

    int fd = Connect(server.bound_port());
    auto stopped = std::async(std::launch::async, [&] { server.Stop(); });
    auto status = stopped.wait_for(std::chrono::seconds(2));
    CHECK(status == std::future_status::ready);

    ::close(fd);
    stopped.wait();
    std::filesystem::remove_all(dir);
}

TEST_CASE("server: authentication gates commands") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    ServerOptions server_opts;
    server_opts.port = 0;
    server_opts.requirepass = "secret";
    Server server(engine, server_opts);
    REQUIRE(server.Start().ok());

    int fd = Connect(server.bound_port());
    SendAll(fd, BulkCommand({"PING"}));
    auto unauth = RecvUntilQuiet(fd);
    CHECK(unauth.rfind("-NOAUTH", 0) == 0);

    SendAll(fd, BulkCommand({"AUTH", "wrong"}));
    auto wrong = RecvUntilQuiet(fd);
    CHECK(wrong.rfind("-ERR", 0) == 0);

    SendAll(fd, BulkCommand({"AUTH", "secret"}));
    auto ok = RecvUntilQuiet(fd);
    CHECK(ok == "+OK\r\n");

    SendAll(fd, BulkCommand({"PING"}));
    auto pong = RecvUntilQuiet(fd);
    CHECK(pong == "+PONG\r\n");

    ::close(fd);
    server.Stop();
    std::filesystem::remove_all(dir);
}

TEST_CASE("server: connection limit and invalid host are rejected") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    ServerOptions bad_host;
    bad_host.host = "not-an-ip";
    Server bad(engine, bad_host);
    auto bad_status = bad.Start();
    CHECK(bad_status.code() == StatusCode::kInvalidArgument);

    ServerOptions server_opts;
    server_opts.port = 0;
    server_opts.max_connections = 1;
    Server server(engine, server_opts);
    REQUIRE(server.Start().ok());

    int first = Connect(server.bound_port());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    int second = Connect(server.bound_port());
    auto rejected = RecvUntilQuiet(second);
    CHECK(rejected.rfind("-ERR", 0) == 0);
    ::close(second);

    ::close(first);
    server.Stop();
    std::filesystem::remove_all(dir);
}

TEST_CASE("engine: scan returns ordered live keys from a start key") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = false;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    REQUIRE(engine.Put("b", "2").ok());
    REQUIRE(engine.Put("a", "1").ok());
    REQUIRE(engine.Put("c", "3").ok());
    REQUIRE(engine.Delete("b").ok());

    std::vector<std::pair<std::string, std::string>> rows;
    REQUIRE(engine.Scan("", 10, &rows).ok());
    REQUIRE(rows.size() == 2);
    CHECK(rows[0].first == "a");
    CHECK(rows[1].first == "c");

    REQUIRE(engine.Scan("b", 10, &rows).ok());
    REQUIRE(rows.size() == 1);
    CHECK(rows[0].first == "c");

    std::filesystem::remove_all(dir);
}

TEST_CASE("server: idle timeout closes a silent client") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    ServerOptions server_opts;
    server_opts.port = 0;
    server_opts.idle_timeout_ms = 100;
    Server server(engine, server_opts);
    REQUIRE(server.Start().ok());

    int fd = Connect(server.bound_port());
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    char byte = 0;
    auto n = ::recv(fd, &byte, 1, 0);
    CHECK(n <= 0);
    ::close(fd);
    server.Stop();
    std::filesystem::remove_all(dir);
}

TEST_CASE("backends: truncate and directory sync work on every implementation") {
    auto dir = MakeTempDir();
    auto path = dir + "/truncate.bin";
    std::string data(256, 'x');

    auto check = [&](std::unique_ptr<IOBackend> backend) {
        FileHandle fh = kInvalidFile;
        REQUIRE(backend->Open(path, true, false, &fh).ok());
        REQUIRE(backend->Write(fh, 0, data.data(), data.size()).ok());
        uint64_t size = 0;
        REQUIRE(backend->FileSize(fh, &size).ok());
        CHECK(size == data.size());
        REQUIRE(backend->Truncate(fh, 64).ok());
        REQUIRE(backend->FileSize(fh, &size).ok());
        CHECK(size == 64);
        REQUIRE(backend->SyncDirectory(dir).ok());
        REQUIRE(backend->Close(fh).ok());
        REQUIRE(backend->Delete(path).ok());
    };

    check(std::make_unique<SyncBackend>());
    check(std::make_unique<ThreadPoolBackend>(2));
    check(std::make_unique<MmapBackend>());

#ifdef KV_HAS_IOURING
    auto uring = std::make_unique<IOUringBackend>();
    REQUIRE(uring->Init().ok());
    check(std::move(uring));
#endif

    std::filesystem::remove_all(dir);
}

TEST_CASE("WAL: a torn batch leaves no prefix") {
    auto dir = MakeTempDir();
    SyncBackend backend;

    {
        WAL wal(&backend, dir);
        REQUIRE(wal.Open().ok());
        REQUIRE(wal.AppendBatch({
            {WALRecordType::kPut, "a", "1"},
            {WALRecordType::kPut, "b", "2"},
            {WALRecordType::kPut, "c", "3"},
        }).ok());
        REQUIRE(wal.Sync().ok());
    }

    auto path = dir + "/wal.log";
    auto size = std::filesystem::file_size(path);
    REQUIRE(size > 10);
    std::filesystem::resize_file(path, size - 5);

    {
        WAL wal(&backend, dir);
        REQUIRE(wal.Open().ok());
        size_t replayed = 0;
        REQUIRE(wal.Replay([&](const WALRecord&) { return true; }, &replayed).ok());
        CHECK(replayed == 0);
        REQUIRE(wal.Append({WALRecordType::kPut, "d", "4"}).ok());
        REQUIRE(wal.Sync().ok());
    }

    {
        WAL wal(&backend, dir);
        REQUIRE(wal.Open().ok());
        std::vector<std::string> keys;
        REQUIRE(wal.Replay([&](const WALRecord& rec) {
            keys.push_back(rec.key);
            return true;
        }).ok());
        REQUIRE(keys.size() == 1);
        CHECK(keys[0] == "d");
    }

    std::filesystem::remove_all(dir);
}

TEST_CASE("engine: PutBatch is recovered as one unit") {
    auto dir = MakeTempDir();
    EngineOptions opts;
    opts.data_dir = dir;
    opts.sync_wal = true;

    {
        auto backend = std::make_unique<SyncBackend>();
        Engine engine(std::move(backend), opts);
        REQUIRE(engine.Open().ok());
        REQUIRE(engine.PutBatch({{"a", "1"}, {"b", "2"}, {"c", "3"}}).ok());
    }

    auto backend = std::make_unique<SyncBackend>();
    Engine reopened(std::move(backend), opts);
    REQUIRE(reopened.Open().ok());
    CHECK(reopened.DbSize() == 3);
    CHECK(reopened.Get("a") == std::optional<std::string>("1"));
    CHECK(reopened.Get("b") == std::optional<std::string>("2"));
    CHECK(reopened.Get("c") == std::optional<std::string>("3"));

    std::filesystem::remove_all(dir);
}

TEST_CASE("power loss: flush recovers at every directory sync point") {
    for (int crash_at : {1, 2, 0}) {
        auto dir = MakeTempDir();
        EngineOptions opts;
        opts.data_dir = dir;
        opts.sync_wal = true;

        {
            auto backend = std::make_unique<CrashBackend>();
            auto* raw = backend.get();
            Engine engine(std::move(backend), opts);
            REQUIRE(engine.Open().ok());
            if (crash_at > 0) raw->arm(crash_at);

            REQUIRE(engine.Put("a", "1").ok());
            REQUIRE(engine.Put("b", "2").ok());
            auto flush_status = engine.FlushMemtable();
            if (crash_at > 0) {
                CHECK(!flush_status.ok());
                CHECK(raw->crashed());
            } else {
                CHECK(flush_status.ok());
                CHECK(!raw->crashed());
            }
        }

        auto backend = std::make_unique<SyncBackend>();
        Engine reopened(std::move(backend), opts);
        REQUIRE(reopened.Open().ok());
        CHECK(reopened.DbSize() == 2);
        CHECK(reopened.Get("a") == std::optional<std::string>("1"));
        CHECK(reopened.Get("b") == std::optional<std::string>("2"));

        std::filesystem::remove_all(dir);
    }
}

#ifdef KV_HAS_TLS
TEST_CASE("server: TLS handshake and RESP roundtrip") {
    auto dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = dir;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    auto cert_dir = std::filesystem::absolute(__FILE__).parent_path() / "certs";
    ServerOptions server_opts;
    server_opts.port = 0;
    server_opts.tls_cert = (cert_dir / "test-cert.pem").string();
    server_opts.tls_key = (cert_dir / "test-key.pem").string();
    Server server(engine, server_opts);
    REQUIRE(server.Start().ok());
    CHECK(server.tls_enabled());

    int fd = Connect(server.bound_port());
    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    REQUIRE(ctx != nullptr);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
    SSL* ssl = SSL_new(ctx);
    REQUIRE(ssl != nullptr);
    SSL_set_fd(ssl, fd);
    REQUIRE(SSL_connect(ssl) == 1);

    auto command = BulkCommand({"PING"});
    REQUIRE(SSL_write(ssl, command.data(), static_cast<int>(command.size())) > 0);
    char reply[64];
    int n = SSL_read(ssl, reply, sizeof(reply));
    REQUIRE(n > 0);
    CHECK(std::string(reply, static_cast<size_t>(n)) == "+PONG\r\n");

    SSL_free(ssl);
    SSL_CTX_free(ctx);
    ::close(fd);
    server.Stop();
    std::filesystem::remove_all(dir);
}
#endif
