#pragma once
// Minimal logging for the KV engine.

#include <cstdio>
#include <cstring>
#include <source_location>
#include <string_view>

namespace kv {

enum class LogLevel : int { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

inline LogLevel& GlobalLogLevel() {
    static LogLevel level = LogLevel::kInfo;
    return level;
}

inline void SetLogLevel(LogLevel level) { GlobalLogLevel() = level; }

inline const char* LevelName(LogLevel level) {
    switch (level) {
        case LogLevel::kDebug: return "DEBUG";
        case LogLevel::kInfo:  return "INFO";
        case LogLevel::kWarn:  return "WARN";
        case LogLevel::kError: return "ERROR";
    }
    return "?";
}

// printf-style logging with source location.
template <typename... Args>
void Log(LogLevel level, std::source_location loc, const char* fmt, Args... args) {
    if (level < GlobalLogLevel()) return;
    // Extract filename from path
    const char* file = loc.file_name();
    if (const char* slash = std::strrchr(file, '/')) file = slash + 1;
    std::fprintf(stderr, "[%s] %s:%u: ", LevelName(level), file, loc.line());
    std::fprintf(stderr, fmt, args...);
    std::fputc('\n', stderr);
}

} // namespace kv

#define KV_LOG_DEBUG(...) ::kv::Log(::kv::LogLevel::kDebug, std::source_location::current(), __VA_ARGS__)
#define KV_LOG_INFO(...)  ::kv::Log(::kv::LogLevel::kInfo,  std::source_location::current(), __VA_ARGS__)
#define KV_LOG_WARN(...)  ::kv::Log(::kv::LogLevel::kWarn,  std::source_location::current(), __VA_ARGS__)
#define KV_LOG_ERROR(...) ::kv::Log(::kv::LogLevel::kError, std::source_location::current(), __VA_ARGS__)
