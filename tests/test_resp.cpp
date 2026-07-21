#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "server/resp.h"

using namespace kv;

TEST_CASE("RESP: parse simple string") {
    auto result = RespParser::Parse("+OK\r\n");
    REQUIRE(result.has_value());
    CHECK(result->first.type == RespType::kSimpleString);
    CHECK(result->first.str == "OK");
    CHECK(result->second == 5);
}

TEST_CASE("RESP: parse error") {
    auto result = RespParser::Parse("-ERR unknown\r\n");
    REQUIRE(result.has_value());
    CHECK(result->first.type == RespType::kError);
    CHECK(result->first.str == "ERR unknown");
}

TEST_CASE("RESP: parse integer") {
    auto result = RespParser::Parse(":42\r\n");
    REQUIRE(result.has_value());
    CHECK(result->first.type == RespType::kInteger);
    CHECK(result->first.integer == 42);
}

TEST_CASE("RESP: parse bulk string") {
    auto result = RespParser::Parse("$5\r\nhello\r\n");
    REQUIRE(result.has_value());
    CHECK(result->first.type == RespType::kBulkString);
    CHECK(result->first.str == "hello");
}

TEST_CASE("RESP: parse null bulk string") {
    auto result = RespParser::Parse("$-1\r\n");
    REQUIRE(result.has_value());
    CHECK(result->first.type == RespType::kBulkString);
    CHECK(result->first.is_null);
}

TEST_CASE("RESP: parse array") {
    auto result = RespParser::Parse("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    REQUIRE(result.has_value());
    CHECK(result->first.type == RespType::kArray);
    REQUIRE(result->first.array.size() == 2);
    CHECK(result->first.array[0].str == "GET");
    CHECK(result->first.array[1].str == "foo");
}

TEST_CASE("RESP: parse command") {
    auto result = RespParser::ParseCommand("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
    REQUIRE(result.has_value());
    auto& [args, consumed] = *result;
    REQUIRE(args.size() == 3);
    CHECK(args[0] == "SET");
    CHECK(args[1] == "key");
    CHECK(args[2] == "value");
}

TEST_CASE("RESP: serialize simple string") {
    CHECK(RespSerializer::Serialize(RespValue::SimpleString("OK")) == "+OK\r\n");
}

TEST_CASE("RESP: serialize bulk string") {
    CHECK(RespSerializer::Serialize(RespValue::BulkString("hello")) == "$5\r\nhello\r\n");
}

TEST_CASE("RESP: serialize null bulk string") {
    CHECK(RespSerializer::Serialize(RespValue::NullBulkString()) == "$-1\r\n");
}

TEST_CASE("RESP: serialize integer") {
    CHECK(RespSerializer::Serialize(RespValue::Integer(42)) == ":42\r\n");
}

TEST_CASE("RESP: serialize array") {
    auto arr = RespValue::Array({
        RespValue::BulkString("GET"),
        RespValue::BulkString("foo"),
    });
    CHECK(RespSerializer::Serialize(arr) == "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
}

TEST_CASE("RESP: roundtrip") {
    std::string input = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    auto parsed = RespParser::Parse(input);
    REQUIRE(parsed.has_value());
    auto serialized = RespSerializer::Serialize(parsed->first);
    CHECK(serialized == input);
}

// ---------------------------------------------------------------------------
// G008 hardening: malformed input must be reported as kError, never throw.
// ---------------------------------------------------------------------------

TEST_CASE("RESP safe: malformed integer is an error, not a crash") {
    auto r = RespParser::ParseCommandSafe(":notanumber\r\n");
    CHECK(r.status == ParseStatus::kError);
}

TEST_CASE("RESP safe: oversized bulk length is an error") {
    // 999999999999 exceeds kMaxBulkLen -> protocol error (no bad_alloc).
    auto r = RespParser::ParseCommandSafe("$999999999999\r\n");
    CHECK(r.status == ParseStatus::kError);
}

TEST_CASE("RESP safe: huge array count cannot trigger bad_alloc") {
    // A bogus huge count with no elements must be an error or incomplete,
    // and must NOT allocate ~count elements.
    auto r = RespParser::ParseCommandSafe("*999999999999\r\n");
    CHECK((r.status == ParseStatus::kError || r.status == ParseStatus::kIncomplete));
}

TEST_CASE("RESP safe: array count over limit is an error") {
    auto r = RespParser::ParseCommandSafe("*2000000\r\n");
    CHECK(r.status == ParseStatus::kError);
}

TEST_CASE("RESP safe: garbage type byte is an error") {
    auto r = RespParser::ParseCommandSafe("%garbage\r\n");
    CHECK(r.status == ParseStatus::kError);
}

TEST_CASE("RESP safe: incomplete input stays incomplete") {
    auto r = RespParser::ParseCommandSafe("*2\r\n$3\r\nGET\r\n");
    CHECK(r.status == ParseStatus::kIncomplete);
}

TEST_CASE("RESP safe: valid command parses ok") {
    auto r = RespParser::ParseCommandSafe("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    REQUIRE(r.status == ParseStatus::kOk);
    REQUIRE(r.args.size() == 3);
    CHECK(r.args[0] == "SET");
    CHECK(r.consumed == std::string("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").size());
}

TEST_CASE("RESP safe: legacy Parse never throws on garbage") {
    // Legacy API maps malformed -> nullopt (must not throw).
    CHECK(!RespParser::Parse(":xyz\r\n").has_value());
    CHECK(!RespParser::Parse("$abc\r\n").has_value());
    CHECK(!RespParser::Parse("@@@\r\n").has_value());
}

TEST_CASE("RESP safe: integer overflow is an error") {
    auto r = RespParser::ParseCommandSafe(":99999999999999999999999999\r\n");
    CHECK(r.status == ParseStatus::kError);
}
