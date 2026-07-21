#pragma once
// RESP2 protocol parser and serializer for Redis-compatible API.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
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

// RESP2 parser: parses a command from a byte buffer.
// Returns nullopt if the buffer doesn't contain a complete command.
class RespParser {
public:
    // Try to parse one RESP value from the buffer.
    // Returns the parsed value and the number of bytes consumed, or nullopt.
    static std::optional<std::pair<RespValue, size_t>> Parse(std::string_view buf) {
        if (buf.empty()) return std::nullopt;
        size_t pos = 0;
        auto val = ParseValue(buf, pos);
        if (!val.has_value()) return std::nullopt;
        return std::make_pair(std::move(*val), pos);
    }

    // Parse a command (array of bulk strings) into a vector of strings.
    static std::optional<std::pair<std::vector<std::string>, size_t>>
    ParseCommand(std::string_view buf) {
        auto result = Parse(buf);
        if (!result.has_value()) return std::nullopt;

        auto& [val, consumed] = *result;
        if (val.type != RespType::kArray) return std::nullopt;

        std::vector<std::string> args;
        args.reserve(val.array.size());
        for (auto& elem : val.array) {
            if (elem.type == RespType::kBulkString && !elem.is_null) {
                args.push_back(elem.str);
            } else if (elem.type == RespType::kSimpleString) {
                args.push_back(elem.str);
            } else if (elem.type == RespType::kInteger) {
                args.push_back(std::to_string(elem.integer));
            }
        }
        return std::make_pair(std::move(args), consumed);
    }

private:
    static std::optional<RespValue> ParseValue(std::string_view buf, size_t& pos) {
        if (pos >= buf.size()) return std::nullopt;

        char type_char = buf[pos];
        auto type = static_cast<RespType>(type_char);

        switch (type) {
            case RespType::kSimpleString: {
                auto line = ReadLine(buf, pos);
                if (!line.has_value()) return std::nullopt;
                return RespValue::SimpleString(std::string(*line));
            }
            case RespType::kError: {
                auto line = ReadLine(buf, pos);
                if (!line.has_value()) return std::nullopt;
                return RespValue::Error(std::string(*line));
            }
            case RespType::kInteger: {
                auto line = ReadLine(buf, pos);
                if (!line.has_value()) return std::nullopt;
                return RespValue::Integer(std::stoll(std::string(*line)));
            }
            case RespType::kBulkString: {
                auto len_line = ReadLine(buf, pos);
                if (!len_line.has_value()) return std::nullopt;
                int64_t len = std::stoll(std::string(*len_line));
                if (len < 0) return RespValue::NullBulkString();
                if (pos + static_cast<size_t>(len) + 2 > buf.size()) return std::nullopt;
                std::string data(buf.substr(pos, static_cast<size_t>(len)));
                pos += static_cast<size_t>(len) + 2;  // Skip data + \r\n
                return RespValue::BulkString(std::move(data));
            }
            case RespType::kArray: {
                auto len_line = ReadLine(buf, pos);
                if (!len_line.has_value()) return std::nullopt;
                int64_t count = std::stoll(std::string(*len_line));
                if (count < 0) return RespValue::NullArray();
                std::vector<RespValue> arr;
                arr.reserve(static_cast<size_t>(count));
                for (int64_t i = 0; i < count; i++) {
                    auto elem = ParseValue(buf, pos);
                    if (!elem.has_value()) return std::nullopt;
                    arr.push_back(std::move(*elem));
                }
                return RespValue::Array(std::move(arr));
            }
            default:
                return std::nullopt;
        }
    }

    static std::optional<std::string_view> ReadLine(std::string_view buf, size_t& pos) {
        auto crlf = buf.find("\r\n", pos);
        if (crlf == std::string_view::npos) return std::nullopt;
        auto line = buf.substr(pos + 1, crlf - pos - 1);  // Skip type char.
        pos = crlf + 2;
        return line;
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
