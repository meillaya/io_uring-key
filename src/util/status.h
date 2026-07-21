#pragma once
// Status: lightweight error/result type for the KV engine.

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace kv {

enum class StatusCode : uint8_t {
    kOk = 0,
    kNotFound,
    kCorruption,
    kIOError,
    kInvalidArgument,
    kAlreadyExists,
    kNotSupported,
};

class Status {
public:
    Status() : code_(StatusCode::kOk) {}
    Status(StatusCode code, std::string msg = {})
        : code_(code), msg_(std::move(msg)) {}

    static Status OK() { return {}; }
    static Status NotFound(std::string msg = {}) { return Status(StatusCode::kNotFound, std::move(msg)); }
    static Status Corruption(std::string msg = {}) { return Status(StatusCode::kCorruption, std::move(msg)); }
    static Status IOError(std::string msg = {}) { return Status(StatusCode::kIOError, std::move(msg)); }
    static Status InvalidArgument(std::string msg = {}) { return Status(StatusCode::kInvalidArgument, std::move(msg)); }
    static Status AlreadyExists(std::string msg = {}) { return Status(StatusCode::kAlreadyExists, std::move(msg)); }
    static Status NotSupported(std::string msg = {}) { return Status(StatusCode::kNotSupported, std::move(msg)); }

    [[nodiscard]] bool ok() const { return code_ == StatusCode::kOk; }
    [[nodiscard]] bool IsNotFound() const { return code_ == StatusCode::kNotFound; }
    [[nodiscard]] bool IsCorruption() const { return code_ == StatusCode::kCorruption; }
    [[nodiscard]] bool IsIOError() const { return code_ == StatusCode::kIOError; }
    [[nodiscard]] StatusCode code() const { return code_; }
    [[nodiscard]] const std::string& message() const { return msg_; }

    [[nodiscard]] std::string ToString() const {
        if (ok()) return "OK";
        std::string_view name;
        switch (code_) {
            case StatusCode::kNotFound: name = "NotFound"; break;
            case StatusCode::kCorruption: name = "Corruption"; break;
            case StatusCode::kIOError: name = "IOError"; break;
            case StatusCode::kInvalidArgument: name = "InvalidArgument"; break;
            case StatusCode::kAlreadyExists: name = "AlreadyExists"; break;
            case StatusCode::kNotSupported: name = "NotSupported"; break;
            default: name = "Unknown"; break;
        }
        if (msg_.empty()) return std::string(name);
        return std::string(name) + ": " + msg_;
    }

private:
    StatusCode code_;
    std::string msg_;
};

} // namespace kv
