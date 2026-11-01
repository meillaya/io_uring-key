#pragma once
// RESP2 protocol parser and serializer for Redis-compatible API.

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kv {

// RESP2 value types.
enum class RespType : uint8_t {
    kSimpleString = '+',
    kError = '-',
    kInteger = ':',
    kBulkString = '$',
    kArray = '*',
};

struct RespValue {
    RespType type = RespType::kSimpleString;
    std::string str;
    int64_t integer = 0;
    std::vector<RespValue> array;
    bool is_null = false;

    static RespValue SimpleString(std::string s) {
        RespValue v; v.type = RespType::kSimpleString; v.str = std::move(s); return v;
    }
    static RespValue Error(std::string s) {
        RespValue v; v.type = RespType::kError; v.str = std::move(s); return v;
    }
    static RespValue Integer(int64_t i) {
        RespValue v; v.type = RespType::kInteger; v.integer = i; return v;
    }
    static RespValue BulkString(std::string s) {
        RespValue v; v.type = RespType::kBulkString; v.str = std::move(s); return v;
    }
    static RespValue NullBulkString() {
        RespValue v; v.type = RespType::kBulkString; v.is_null = true; return v;
    }
    static RespValue Array(std::vector<RespValue> arr) {
        RespValue v; v.type = RespType::kArray; v.array = std::move(arr); return v;
    }
    static RespValue NullArray() {
        RespValue v; v.type = RespType::kArray; v.is_null = true; return v;
    }
};

// Outcome of a parse attempt: distinguishes "need more bytes" from "malformed".
enum class ParseStatus : uint8_t {
    kOk,          // A complete value/command was parsed.
    kIncomplete,  // Buffer lacks a full frame; caller should read more bytes.
    kError,       // Malformed protocol input; caller should reply -ERR and reset.
};

// Result of parsing one command (an array of bulk strings).
struct ParsedCommand {
    ParseStatus status = ParseStatus::kIncomplete;
    std::vector<std::string> args;
    size_t consumed = 0;
    std::string error;  // Populated when status == kError.
};

// RESP2 parser: parses a command from a byte buffer.
//
// Hardening contract (G008): the parser NEVER throws. Malformed input
// (bad integers, oversized bulk/array lengths, garbage type bytes) is
// reported as ParseStatus::kError instead of escaping as std::stoll /
// std::bad_alloc, so a hostile client cannot crash the server.
class RespParser {
public:
    // Protocol safety limits (mirrors Redis defaults / sane bounds).
    static constexpr int64_t kMaxBulkLen = 512 * 1024 * 1024;   // 512 MB
    static constexpr int64_t kMaxArrayCount = 1024 * 1024;      // 1M elements
    static constexpr int64_t kMaxInlineLine = 64 * 1024;        // 64 KB

    // Legacy: parse one RESP value. Returns nullopt on incomplete OR malformed
    // input (kept for backward compatibility with existing callers/tests).
    static std::optional<std::pair<RespValue, size_t>> Parse(std::string_view buf) {
        size_t pos = 0;
        std::string err;
        auto val = ParseValue(buf, pos, err);
        if (val.status != ParseStatus::kOk) return std::nullopt;
        return std::make_pair(std::move(val.value), pos);
    }

    // Legacy: parse a command into a vector of strings. Returns nullopt on
    // incomplete OR malformed input.
    static std::optional<std::pair<std::vector<std::string>, size_t>>
    ParseCommand(std::string_view buf) {
        auto pc = ParseCommandSafe(buf);
        if (pc.status != ParseStatus::kOk) return std::nullopt;
        return std::make_pair(std::move(pc.args), pc.consumed);
    }

    // Safe command parser used by the server: distinguishes incomplete from
    // malformed so the server can wait for more bytes vs. reply -ERR.
    static ParsedCommand ParseCommandSafe(std::string_view buf) {
        ParsedCommand out;
        if (buf.empty()) return out;  // kIncomplete.

        size_t pos = 0;
        std::string err;
        auto val = ParseValue(buf, pos, err);
        if (val.status == ParseStatus::kIncomplete) return out;  // need more bytes
        if (val.status == ParseStatus::kError) {
            out.status = ParseStatus::kError;
            out.error = err.empty() ? "protocol error" : err;
            out.consumed = pos;  // best-effort; server resets the connection anyway
            return out;
        }

        if (val.value.type != RespType::kArray) {
            out.status = ParseStatus::kError;
            out.error = "expected array command";
            out.consumed = pos;
            return out;
        }

        out.args.reserve(val.value.array.size());
        for (auto& elem : val.value.array) {
            if (elem.type == RespType::kBulkString && !elem.is_null) {
                out.args.push_back(elem.str);
            } else if (elem.type == RespType::kSimpleString) {
                out.args.push_back(elem.str);
            } else if (elem.type == RespType::kInteger) {
                out.args.push_back(std::to_string(elem.integer));
            }
            // Other element types are ignored (not valid in a command array).
        }
        out.status = ParseStatus::kOk;
        out.consumed = pos;
        return out;
    }

private:
    struct ValueResult {
        ParseStatus status = ParseStatus::kIncomplete;
        RespValue value;
    };

    // Parse a signed 64-bit integer from a decimal line. Returns false on any
    // non-digit (other than a single leading '-') or overflow. NEVER throws.
    static bool ParseInt64(std::string_view s, int64_t* out) {
        if (s.empty()) return false;
        size_t i = 0;
        bool neg = false;
        if (s[0] == '-' || s[0] == '+') {
            neg = (s[0] == '-');
            i = 1;
            if (i >= s.size()) return false;
        }
        int64_t val = 0;
        const int64_t limit = neg ? std::numeric_limits<int64_t>::min()
                                  : -std::numeric_limits<int64_t>::max();
        const int64_t base_limit = limit / 10;
        for (; i < s.size(); i++) {
            char c = s[i];
            if (c < '0' || c > '9') return false;
            int digit = c - '0';
            if (val < base_limit || (val == base_limit && -digit < limit % 10)) {
                return false;  // overflow
            }
            val = val * 10 - digit;
        }
        *out = neg ? val : -val;
        return true;
    }

    static ValueResult ParseValue(std::string_view buf, size_t& pos, std::string& err) {
        ValueResult r;
        if (pos >= buf.size()) return r;  // kIncomplete

        char type_char = buf[pos];
        auto type = static_cast<RespType>(type_char);

        switch (type) {
            case RespType::kSimpleString: {
                auto line = ReadLine(buf, pos, err);
                if (line.status != ParseStatus::kOk) { r.status = line.status; return r; }
                r.status = ParseStatus::kOk;
                r.value = RespValue::SimpleString(std::string(line.line));
                return r;
            }
            case RespType::kError: {
                auto line = ReadLine(buf, pos, err);
                if (line.status != ParseStatus::kOk) { r.status = line.status; return r; }
                r.status = ParseStatus::kOk;
                r.value = RespValue::Error(std::string(line.line));
                return r;
            }
            case RespType::kInteger: {
                auto line = ReadLine(buf, pos, err);
                if (line.status != ParseStatus::kOk) { r.status = line.status; return r; }
                int64_t v;
                if (!ParseInt64(line.line, &v)) {
                    r.status = ParseStatus::kError;
                    err = "invalid integer";
                    return r;
                }
                r.status = ParseStatus::kOk;
                r.value = RespValue::Integer(v);
                return r;
            }
            case RespType::kBulkString: {
                auto len_line = ReadLine(buf, pos, err);
                if (len_line.status != ParseStatus::kOk) { r.status = len_line.status; return r; }
                int64_t len;
                if (!ParseInt64(len_line.line, &len)) {
                    r.status = ParseStatus::kError;
                    err = "invalid bulk length";
                    return r;
                }
                if (len < -1) {
                    r.status = ParseStatus::kError;
                    err = "invalid bulk length";
                    return r;
                }
                if (len == -1) {
                    r.status = ParseStatus::kOk;
                    r.value = RespValue::NullBulkString();
                    return r;
                }
                if (len > kMaxBulkLen) {
                    r.status = ParseStatus::kError;
                    err = "bulk length exceeds limit";
                    return r;
                }
                if (pos + static_cast<size_t>(len) + 2 > buf.size()) return r;  // kIncomplete
                // Validate trailing CRLF.
                if (buf[pos + static_cast<size_t>(len)] != '\r' ||
                    buf[pos + static_cast<size_t>(len) + 1] != '\n') {
                    r.status = ParseStatus::kError;
                    err = "missing bulk CRLF";
                    return r;
                }
                r.status = ParseStatus::kOk;
                r.value = RespValue::BulkString(
                    std::string(buf.substr(pos, static_cast<size_t>(len))));
                pos += static_cast<size_t>(len) + 2;
                return r;
            }
            case RespType::kArray: {
                auto len_line = ReadLine(buf, pos, err);
                if (len_line.status != ParseStatus::kOk) { r.status = len_line.status; return r; }
                int64_t count;
                if (!ParseInt64(len_line.line, &count)) {
                    r.status = ParseStatus::kError;
                    err = "invalid array count";
                    return r;
                }
                if (count < -1) {
                    r.status = ParseStatus::kError;
                    err = "invalid array count";
                    return r;
                }
                if (count == -1) {
                    r.status = ParseStatus::kOk;
                    r.value = RespValue::NullArray();
                    return r;
                }
                if (count > kMaxArrayCount) {
                    r.status = ParseStatus::kError;
                    err = "array count exceeds limit";
                    return r;
                }
                std::vector<RespValue> arr;
                // Cap the upfront reservation so a bogus huge count cannot
                // trigger std::bad_alloc before we validate the elements.
                arr.reserve(static_cast<size_t>(std::min<int64_t>(count, 1024)));
                for (int64_t i = 0; i < count; i++) {
                    auto elem = ParseValue(buf, pos, err);
                    if (elem.status == ParseStatus::kIncomplete) return r;  // kIncomplete
                    if (elem.status == ParseStatus::kError) { r.status = ParseStatus::kError; return r; }
                    arr.push_back(std::move(elem.value));
                }
                r.status = ParseStatus::kOk;
                r.value = RespValue::Array(std::move(arr));
                return r;
            }
            default:
                r.status = ParseStatus::kError;
                err = std::string("unknown type byte '") + type_char + "'";
                return r;
        }
    }

    struct LineResult {
        ParseStatus status = ParseStatus::kIncomplete;
        std::string_view line;
    };

    static LineResult ReadLine(std::string_view buf, size_t& pos, std::string& err) {
        LineResult r;
        auto crlf = buf.find("\r\n", pos);
        if (crlf == std::string_view::npos) {
            // Do not wait forever for a terminator that may never arrive.
            if (buf.size() - pos > static_cast<size_t>(kMaxInlineLine) + 1) {
                r.status = ParseStatus::kError;
                err = "line too long";
            }
            return r;  // kIncomplete unless the line crossed the cap
        }
        if (static_cast<int64_t>(crlf - pos) > kMaxInlineLine + 1) {
            r.status = ParseStatus::kError;
            err = "line too long";
            return r;
        }
        r.status = ParseStatus::kOk;
        r.line = buf.substr(pos + 1, crlf - pos - 1);  // Skip type char.
        pos = crlf + 2;
        return r;
    }
};

// RESP2 serializer.
class RespSerializer {
public:
    static std::string Serialize(const RespValue& val) {
        std::string out;
        SerializeValue(val, out);
        return out;
    }

private:
    static void SerializeValue(const RespValue& val, std::string& out) {
        switch (val.type) {
            case RespType::kSimpleString:
                out += '+';
                out += val.str;
                out += "\r\n";
                break;
            case RespType::kError:
                out += '-';
                out += val.str;
                out += "\r\n";
                break;
            case RespType::kInteger:
                out += ':';
                out += std::to_string(val.integer);
                out += "\r\n";
                break;
            case RespType::kBulkString:
                if (val.is_null) {
                    out += "$-1\r\n";
                } else {
                    out += '$';
                    out += std::to_string(val.str.size());
                    out += "\r\n";
                    out += val.str;
                    out += "\r\n";
                }
                break;
            case RespType::kArray:
                if (val.is_null) {
                    out += "*-1\r\n";
                } else {
                    out += '*';
                    out += std::to_string(val.array.size());
                    out += "\r\n";
                    for (auto& elem : val.array) {
                        SerializeValue(elem, out);
                    }
                }
                break;
        }
    }
};

} // namespace kv
