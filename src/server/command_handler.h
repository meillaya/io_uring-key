#pragma once
// Redis-compatible command handler: bounded RESP2 command set.

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "engine/engine.h"
#include "server/resp.h"
#include "util/logging.h"

namespace kv {

class CommandHandler {
public:
    explicit CommandHandler(Engine& engine) : engine_(engine) {}

    RespValue Execute(const std::vector<std::string>& args) {
        if (args.empty()) return RespValue::Error("ERR empty command");

        std::string cmd = args[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

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
        if (cmd == "SELECT") return RespValue::SimpleString("OK");
        if (cmd == "COMMAND") return RespValue::Array({});
        if (cmd == "INFO") return CmdInfo(args);
        if (cmd == "SCAN") return CmdScan(args);

        return RespValue::Error("ERR unknown command '" + args[0] + "'");
    }

private:
    RespValue CmdPing(const std::vector<std::string>& args) {
        if (args.size() > 1) return RespValue::BulkString(args[1]);
        return RespValue::SimpleString("PONG");
    }

    RespValue CmdEcho(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::Error("ERR wrong number of arguments for 'echo'");
        return RespValue::BulkString(args[1]);
    }

    RespValue CmdSet(const std::vector<std::string>& args) {
        if (args.size() < 3) return RespValue::Error("ERR wrong number of arguments for 'set'");

        const std::string& key = args[1];
        const std::string& value = args[2];

        // Parse options: NX, XX, EX <seconds>.
        bool nx = false, xx = false;
        for (size_t i = 3; i < args.size(); i++) {
            std::string opt = args[i];
            std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
            if (opt == "NX") nx = true;
            else if (opt == "XX") xx = true;
            else if (opt == "EX" && i + 1 < args.size()) i++;  // Skip TTL (not implemented).
        }

        auto existing = engine_.Get(key);
        if (nx && existing.has_value()) return RespValue::NullBulkString();
        if (xx && !existing.has_value()) return RespValue::NullBulkString();

        auto s = engine_.Put(key, value);
        if (!s.ok()) return RespValue::Error("ERR " + s.ToString());
        return RespValue::SimpleString("OK");
    }

    RespValue CmdGet(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::Error("ERR wrong number of arguments for 'get'");
        std::optional<std::string> val;
        auto st = engine_.Get(args[1], &val);
        if (!st.ok()) return RespValue::Error("ERR " + st.ToString());  // e.g. Corruption
        if (!val.has_value()) return RespValue::NullBulkString();
        return RespValue::BulkString(*val);
    }

    RespValue CmdDel(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::Error("ERR wrong number of arguments for 'del'");
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); i++) {
            auto existing = engine_.Get(args[i]);
            if (existing.has_value()) {
                engine_.Delete(args[i]);
                count++;
            }
        }
        return RespValue::Integer(count);
    }

    RespValue CmdExists(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::Error("ERR wrong number of arguments for 'exists'");
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); i++) {
            if (engine_.Get(args[i]).has_value()) count++;
        }
        return RespValue::Integer(count);
    }

    RespValue CmdMget(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::Error("ERR wrong number of arguments for 'mget'");
        std::vector<RespValue> results;
        for (size_t i = 1; i < args.size(); i++) {
            auto val = engine_.Get(args[i]);
            if (val.has_value()) {
                results.push_back(RespValue::BulkString(*val));
            } else {
                results.push_back(RespValue::NullBulkString());
            }
        }
        return RespValue::Array(std::move(results));
    }

    RespValue CmdMset(const std::vector<std::string>& args) {
        if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
            return RespValue::Error("ERR wrong number of arguments for 'mset'");
        }
        for (size_t i = 1; i + 1 < args.size(); i += 2) {
            engine_.Put(args[i], args[i + 1]);
        }
        return RespValue::SimpleString("OK");
    }

    RespValue CmdDbsize(const std::vector<std::string>&) {
        // Count live keys across memtable + SSTables (newest version wins).
        return RespValue::Integer(static_cast<int64_t>(engine_.DbSize()));
    }

    RespValue CmdFlushdb(const std::vector<std::string>&) {
        // FLUSHDB (G008): delete ALL keys — clear the active memtable, remove
        // every SSTable file, and truncate the WAL. Scoped to the engine's own
        // data dir only. After this DBSIZE returns 0 and prior keys are gone.
        KV_LOG_WARN("FLUSHDB: clearing engine data");
        auto st = engine_.Clear();
        if (!st.ok()) return RespValue::Error("ERR " + st.ToString());
        return RespValue::SimpleString("OK");
    }

    RespValue CmdInfo(const std::vector<std::string>&) {
        std::string info = "# Server\r\n"
                          "kv_engine:kernel-v2\r\n"
                          "io_backend:" + std::string(engine_.backend()->Name()) + "\r\n"
                          "# Keyspace\r\n"
                          "db0:keys=" + std::to_string(engine_.DbSize()) + ",expires=0\r\n";
        return RespValue::BulkString(info);
    }

    RespValue CmdScan(const std::vector<std::string>& args) {
        // Minimal SCAN: return cursor 0 and empty list.
        std::vector<RespValue> result;
        result.push_back(RespValue::BulkString("0"));  // Next cursor.
        result.push_back(RespValue::Array({}));         // Empty keys list.
        return RespValue::Array(std::move(result));
    }

    Engine& engine_;
};

} // namespace kv
