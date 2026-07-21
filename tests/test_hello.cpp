#include <doctest/doctest.h>

TEST_CASE("hello world - scaffold sanity") {
    CHECK(1 + 1 == 2);
}

TEST_CASE("C++23 feature check") {
    // Verify we're actually compiling as C++23
    CHECK(__cplusplus >= 202302L);
}
