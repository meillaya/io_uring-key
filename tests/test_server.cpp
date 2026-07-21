#include <doctest/doctest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include "engine/engine.h"
#include "io/sync_backend.h"
#include "server/server.h"

using namespace kv;

namespace {

std::string MakeTempDir() {
    char tmpl[] = "/tmp/kv_server_test_XXXXXX";
    char* dir = mkdtemp(tmpl);
    REQUIRE(dir != nullptr);
    return std::string(dir);
}

int Connect(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd >= 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    int rc = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    REQUIRE(rc == 0);
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

// Read until the socket closes or a short timeout with no data.
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

} // namespace

TEST_CASE("server: malformed RESP never crashes the server") {
    auto data_dir = MakeTempDir();
    auto backend = std::make_unique<SyncBackend>();
    EngineOptions opts;
    opts.data_dir = data_dir;
    Engine engine(std::move(backend), opts);
    REQUIRE(engine.Open().ok());

    ServerOptions sopts;
    sopts.host = "127.0.0.1";
    sopts.port = 0;  // ephemeral
    Server server(engine, sopts);
    REQUIRE(server.Start().ok());
    uint16_t port = server.bound_port();
    REQUIRE(port != 0);

    SUBCASE("valid PING works") {
        int fd = Connect(port);
        SendAll(fd, "*1\r\n$4\r\nPING\r\n");
        auto reply = RecvUntilQuiet(fd);
        ::close(fd);
        CHECK(reply == "+PONG\r\n");
    }

    SUBCASE("bad integer yields -ERR and server stays up") {
        int fd = Connect(port);
        SendAll(fd, ":notanumber\r\n");
        auto reply = RecvUntilQuiet(fd);
        ::close(fd);
        CHECK(reply.rfind("-ERR", 0) == 0);

        // Server must still accept and serve a fresh, valid connection.
        int fd2 = Connect(port);
        SendAll(fd2, "*1\r\n$4\r\nPING\r\n");
        auto reply2 = RecvUntilQuiet(fd2);
        ::close(fd2);
        CHECK(reply2 == "+PONG\r\n");
    }

    SUBCASE("huge bulk length yields -ERR (no bad_alloc)") {
        int fd = Connect(port);
        SendAll(fd, "$999999999999\r\n");
        auto reply = RecvUntilQuiet(fd);
        ::close(fd);
        CHECK(reply.rfind("-ERR", 0) == 0);

        int fd2 = Connect(port);
        SendAll(fd2, "*1\r\n$4\r\nPING\r\n");
        auto reply2 = RecvUntilQuiet(fd2);
        ::close(fd2);
        CHECK(reply2 == "+PONG\r\n");
    }

    SUBCASE("garbage type byte yields -ERR") {
        int fd = Connect(port);
        SendAll(fd, "%garbage\r\n");
        auto reply = RecvUntilQuiet(fd);
        ::close(fd);
        CHECK(reply.rfind("-ERR", 0) == 0);
    }

    SUBCASE("SET then GET roundtrip over the wire") {
        int fd = Connect(port);
        SendAll(fd, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        auto r1 = RecvUntilQuiet(fd, 150);
        CHECK(r1 == "+OK\r\n");
        SendAll(fd, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        auto r2 = RecvUntilQuiet(fd, 150);
        CHECK(r2 == "$3\r\nbar\r\n");
        ::close(fd);
    }

    server.Stop();
    std::filesystem::remove_all(data_dir);
}
