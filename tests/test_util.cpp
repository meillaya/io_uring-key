#include <doctest/doctest.h>

#include <cstring>
#include <string>

#include "util/aligned_buffer.h"
#include "util/crc32.h"
#include "util/histogram.h"
#include "util/status.h"

using namespace kv;

TEST_CASE("Status: basic operations") {
    CHECK(Status::OK().ok());
    CHECK(!Status::NotFound("missing").ok());
    CHECK(Status::NotFound("missing").IsNotFound());
    CHECK(Status::Corruption("bad").IsCorruption());
    CHECK(Status::IOError("disk").IsIOError());
    CHECK(Status::OK().ToString() == "OK");
    CHECK(Status::NotFound("key1").ToString() == "NotFound: key1");
}

TEST_CASE("CRC32: known values") {
    // CRC32C of empty string.
    CHECK(CRC32::Compute("", 0) == 0);

    // CRC32C of "123456789" should be 0xE3069283 (Castagnoli).
    uint32_t crc = CRC32::Compute("123456789", 9);
    CHECK(crc == 0xE3069283);

    // Deterministic: same input = same output.
    std::string data = "hello world, this is a test of CRC32C";
    CHECK(CRC32::Compute(data.data(), data.size()) ==
          CRC32::Compute(data.data(), data.size()));

    // Different input = different output (with high probability).
    std::string data2 = data;
    data2[0] = 'H';
    CHECK(CRC32::Compute(data.data(), data.size()) !=
          CRC32::Compute(data2.data(), data2.size()));
}

TEST_CASE("AlignedBuffer: alignment and operations") {
    AlignedBuffer buf(100);
    CHECK(buf.size() == 100);
    CHECK(buf.capacity() >= 100);
    CHECK(buf.capacity() % kAlignment == 0);
    CHECK(buf.is_aligned());

    // Write and read back.
    std::memset(buf.data(), 0xAB, buf.size());
    CHECK(buf[0] == 0xAB);
    CHECK(buf[99] == 0xAB);

    // Resize.
    buf.Resize(200);
    CHECK(buf.size() == 200);
    CHECK(buf.is_aligned());

    // Append.
    AlignedBuffer buf2;
    buf2.Resize(0);
    const char* msg = "hello";
    buf2.Append(msg, 5);
    CHECK(buf2.size() == 5);
    CHECK(std::memcmp(buf2.data(), "hello", 5) == 0);
}

TEST_CASE("AlignUp: correctness") {
    CHECK(AlignUp(0) == 0);
    CHECK(AlignUp(1) == 4096);
    CHECK(AlignUp(4095) == 4096);
    CHECK(AlignUp(4096) == 4096);
    CHECK(AlignUp(4097) == 8192);
    CHECK(AlignUp(8192) == 8192);
}

TEST_CASE("LatencyHistogram: percentiles") {
    LatencyHistogram hist;

    // Record 100 values: 1, 2, 3, ..., 100.
    for (uint64_t i = 1; i <= 100; i++) {
        hist.Record(i);
    }

    CHECK(hist.Count() == 100);
    CHECK(hist.Min() == 1);
    CHECK(hist.Max() == 100);
    CHECK(hist.Mean() == doctest::Approx(50.5));

    // P50 should be around 50.
    CHECK(hist.P50() >= 32);
    CHECK(hist.P50() <= 63);

    // P99 should be around 99-100.
    CHECK(hist.P99() >= 64);
    CHECK(hist.P99() <= 127);
}
