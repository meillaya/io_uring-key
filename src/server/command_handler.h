#pragma once
// Redis-compatible command handler: bounded RESP2 command set.

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <fnmatch.h>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "engine/kv_store.h"
#include "server/resp.h"
#include "util/logging.h"

namespace kv {

class CommandHandler {
public:
    explicit CommandHandler(KVStore& engine) : engine_(engine) {}

    RespValue Execute(const std::vector<std::string>& args) {
        if (args.empty()) return RespValue::Error("ERR empty command");

        std::string cmd = Upper(args[0]);

        if (cmd == "PING") return CmdPing(args);
        if (cmd == "ECHO") return CmdEcho(args);
        if (cmd == "SET") return CmdSet(args);
        if (cmd == "GET") return CmdGet(args);
        if (cmd == "DEL") return CmdDel(args);
        if (cmd == "EXISTS") return CmdExists(args);
        if (cmd == "MGET") return CmdMget(args);
        if (cmd == "MSET") return CmdMset(args);
        if (cmd == "DBSIZE") return CmdDbsize(args);
        if (cmd == "FLUSHDB") return CmdFlushdb(args);
        if (cmd == "SELECT") return CmdSelect(args);
        if (cmd == "COMMAND") return RespValue::Array({});
        if (cmd == "INFO") return CmdInfo(args);
        if (cmd == "SCAN") return CmdScan(args);
        if (cmd == "AUTH") {
            return RespValue::Error("ERR AUTH called without any password configured");
        }

        return RespValue::Error("ERR unknown command '" + args[0] + "'");
    }

private:
    static std::string Upper(std::string value) {
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(::toupper(c)); });
        return value;
    }

    static bool ParseU64(std::string_view text, uint64_t* out) {
        if (text.empty()) return false;
        uint64_t parsed = 0;
        auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (ec != std::errc() || ptr != text.data() + text.size()) return false;
        *out = parsed;
        return true;
    }

    static bool ParseI64(std::string_view text, int64_t* out) {
        if (text.empty()) return false;
        int64_t parsed = 0;
        auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), parsed);
        if (ec != std::errc() || ptr != text.data() + text.size()) return false;
        *out = parsed;
        return true;
    }

    static RespValue WrongArity(std::string_view command) {
        return RespValue::Error("ERR wrong number of arguments for '" +
                                std::string(command) + "'");
    }

    RespValue CmdPing(const std::vector<std::string>& args) {
        if (args.size() > 2) return WrongArity("ping");
        if (args.size() == 2) return RespValue::BulkString(args[1]);
        return RespValue::SimpleString("PONG");
    }

    RespValue CmdEcho(const std::vector<std::string>& args) {
        if (args.size() != 2) return WrongArity("echo");
        return RespValue::BulkString(args[1]);
    }

    RespValue CmdSet(const std::vector<std::string>& args) {
        if (args.size() < 3) return WrongArity("set");

        const std::string& key = args[1];
        const std::string& value = args[2];

        bool nx = false;
        bool xx = false;
        for (size_t i = 3; i < args.size(); i++) {
            std::string opt = Upper(args[i]);
            if (opt == "NX") {
                if (xx) return RespValue::Error("ERR syntax error");
                nx = true;
            } else if (opt == "XX") {
                if (nx) return RespValue::Error("ERR syntax error");
                xx = true;
            } else if (opt == "EX") {
                if (i + 1 >= args.size()) return RespValue::Error("ERR syntax error");
                int64_t seconds = 0;
                if (!ParseI64(args[i + 1], &seconds) || seconds <= 0) {
                    return RespValue::Error("ERR invalid expire time in 'set' command");
                }
                return RespValue::Error("ERR EX is not supported");
            } else {
                return RespValue::Error("ERR syntax error");
            }
        }

        if (nx) {
            bool inserted = false;
            auto s = engine_.PutIfAbsent(key, value, &inserted);
            if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
            return inserted ? RespValue::SimpleString("OK") : RespValue::NullBulkString();
        }
        if (xx) {
            bool inserted = false;
            auto s = engine_.PutIfPresent(key, value, &inserted);
            if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
            return inserted ? RespValue::SimpleString("OK") : RespValue::NullBulkString();
        }

        auto s = engine_.Put(key, value);
        if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
        return RespValue::SimpleString("OK");
    }

    RespValue CmdGet(const std::vector<std::string>& args) {
        if (args.size() != 2) return WrongArity("get");
        std::optional<std::string> val;
        auto st = engine_.Get(args[1], &val);
        if (!st.ok()) return RespValue::Error("ERR " + st.ToString());
        if (!val.has_value()) return RespValue::NullBulkString();
        return RespValue::BulkString(*val);
    }

    RespValue CmdDel(const std::vector<std::string>& args) {
        if (args.size() < 2) return WrongArity("del");
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); i++) {
            std::optional<std::string> existing;
            auto read_status = engine_.Get(args[i], &existing);
            if (!read_status.ok()) return RespValue::Error("ERR " + read_status.ToString());
            if (!existing.has_value()) continue;

            auto delete_status = engine_.Delete(args[i]);
            if (!delete_status.ok()) {
                return RespValue::Error("ERR " + delete_status.ToString());
            }
            count++;
        }
        return RespValue::Integer(count);
    }

    RespValue CmdExists(const std::vector<std::string>& args) {
        if (args.size() < 2) return WrongArity("exists");
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); i++) {
            std::optional<std::string> value;
            auto s = engine_.Get(args[i], &value);
            if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
            if (value.has_value()) count++;
        }
        return RespValue::Integer(count);
    }

    RespValue CmdMget(const std::vector<std::string>& args) {
        if (args.size() < 2) return WrongArity("mget");
        std::vector<RespValue> results;
        results.reserve(args.size() - 1);
        for (size_t i = 1; i < args.size(); i++) {
            std::optional<std::string> value;
            auto s = engine_.Get(args[i], &value);
            if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
            if (value.has_value()) {
                results.push_back(RespValue::BulkString(*value));
            } else {
                results.push_back(RespValue::NullBulkString());
            }
        }
        return RespValue::Array(std::move(results));
    }

    RespValue CmdMset(const std::vector<std::string>& args) {
        if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
            return WrongArity("mset");
        }

        std::vector<std::pair<std::string, std::string>> entries;
        entries.reserve((args.size() - 1) / 2);
        for (size_t i = 1; i + 1 < args.size(); i += 2) {
            entries.emplace_back(args[i], args[i + 1]);
        }

        auto s = engine_.PutBatch(entries);
        if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
        return RespValue::SimpleString("OK");
    }

    RespValue CmdDbsize(const std::vector<std::string>& args) {
        if (args.size() != 1) return WrongArity("dbsize");
        size_t count = 0;
        auto s = engine_.DbSize(&count);
        if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
        return RespValue::Integer(static_cast<int64_t>(count));
    }

    RespValue CmdFlushdb(const std::vector<std::string>& args) {
        if (args.size() != 1) return WrongArity("flushdb");
        KV_LOG_WARN("FLUSHDB: clearing engine data");
        auto st = engine_.Clear();
        if (!st.ok()) return RespValue::Error("ERR " + st.ToString());
        return RespValue::SimpleString("OK");
    }

    RespValue CmdSelect(const std::vector<std::string>& args) {
        if (args.size() != 2) return WrongArity("select");
        int64_t index = 0;
        if (!ParseI64(args[1], &index) || index < 0) {
            return RespValue::Error("ERR invalid DB index");
        }
        if (index != 0) return RespValue::Error("ERR DB index is out of range");
        return RespValue::SimpleString("OK");
    }

    RespValue CmdInfo(const std::vector<std::string>& args) {
        if (args.size() != 1) return WrongArity("info");
        size_t keys = 0;
        auto s = engine_.DbSize(&keys);
        if (!s.ok()) return RespValue::Error("ERR " + s.ToString());

        std::string info = "# Server\r\n"
                           "kv_engine:kernel-v2\r\n"
                           "role:" + std::string(engine_.is_leader() ? "leader" : "replica") + "\r\n"
                           "io_backend:" + std::string(engine_.backend()->Name()) + "\r\n"
                           "# Keyspace\r\n"
                           "db0:keys=" + std::to_string(keys) + ",expires=0\r\n";
        return RespValue::BulkString(info);
    }

    static std::string HexEncode(std::string_view data) {
        static constexpr char kHex[] = "0123456789abcdef";
        std::string out;
        out.reserve(data.size() * 2);
        for (unsigned char value : data) {
            out.push_back(kHex[value >> 4]);
            out.push_back(kHex[value & 0x0F]);
        }
        return out;
    }

    static bool HexDecode(std::string_view text, std::string* out) {
        if (text.size() % 2 != 0) return false;
        auto nibble = [](char value) -> int {
            if (value >= '0' && value <= '9') return value - '0';
            if (value >= 'a' && value <= 'f') return value - 'a' + 10;
            if (value >= 'A' && value <= 'F') return value - 'A' + 10;
            return -1;
        };
        out->clear();
        out->reserve(text.size() / 2);
        for (size_t i = 0; i < text.size(); i += 2) {
            int high = nibble(text[i]);
            int low = nibble(text[i + 1]);
            if (high < 0 || low < 0) return false;
            out->push_back(static_cast<char>((high << 4) | low));
        }
        return true;
    }

    RespValue CmdScan(const std::vector<std::string>& args) {
        if (args.size() < 2) return WrongArity("scan");

        std::string start_key;
        bool inclusive = true;
        if (args[1] != "0" && !args[1].empty()) {
            if (!HexDecode(args[1], &start_key)) {
                return RespValue::Error("ERR invalid cursor");
            }
            inclusive = false;
        }

        uint64_t count = 10;
        std::string pattern = "*";
        for (size_t i = 2; i < args.size(); i++) {
            std::string opt = Upper(args[i]);
            if (opt == "MATCH" && i + 1 < args.size()) {
                pattern = args[++i];
            } else if (opt == "COUNT" && i + 1 < args.size()) {
                if (!ParseU64(args[++i], &count) || count == 0) {
                    return RespValue::Error("ERR invalid COUNT");
                }
                count = std::min<uint64_t>(count, 10000);
            } else {
                return RespValue::Error("ERR syntax error");
            }
        }

        std::vector<RespValue> found;
        std::string next;
        bool done = false;

        while (found.size() < count) {
            std::vector<std::pair<std::string, std::string>> rows;
            bool batch_done = false;
            std::string batch_next;
            auto s = engine_.ScanAfter(start_key, inclusive, count, &rows,
                                       &batch_done, &batch_next);
            if (!s.ok()) return RespValue::Error("ERR " + s.ToString());

            for (auto& [key, _] : rows) {
                if (pattern == "*" || ::fnmatch(pattern.c_str(), key.c_str(), 0) == 0) {
                    found.push_back(RespValue::BulkString(key));
                }
            }

            next = std::move(batch_next);
            if (batch_done) {
                done = true;
                break;
            }
            if (next.empty()) {
                done = true;
                break;
            }
            start_key = next;
            inclusive = false;
        }

        std::string cursor = done ? "0" : HexEncode(next);
        return RespValue::Array({
            RespValue::BulkString(cursor),
            RespValue::Array(std::move(found)),
        });
    }

    KVStore& engine_;
};

} // namespace kv
