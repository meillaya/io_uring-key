cmake_minimum_required(VERSION 3.28)
project(kernel_v2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

option(KV_BUILD_BENCH "Build benchmarks" ON)
option(KV_USE_IOURING "Enable io_uring backend" ON)
option(KV_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(KV_TSAN "Build with ThreadSanitizer" OFF)

if(KV_SANITIZE AND KV_TSAN)
  message(FATAL_ERROR "KV_SANITIZE and KV_TSAN are mutually exclusive")
endif()

if(KV_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
endif()

if(KV_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=thread)
endif()

if(KV_USE_IOURING)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing>=2.14)
endif()

include(FetchContent)
FetchContent_Declare(
  doctest
  GIT_REPOSITORY https://github.com/doctest/doctest.git
  # v2.4.11, pinned to the commit so a retagged release cannot change the build.
  GIT_TAG        ae7a13539fb71f270b87eb2e874fbac80bc8dda2
)
FetchContent_MakeAvailable(doctest)

add_library(kv_core INTERFACE)
target_include_directories(kv_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(kv_core INTERFACE cxx_std_23)

if(KV_USE_IOURING)
  target_link_libraries(kv_core INTERFACE PkgConfig::LIBURING)
  target_compile_definitions(kv_core INTERFACE KV_HAS_IOURING=1)
endif()

add_executable(kv_server src/main.cpp)
target_link_libraries(kv_server PRIVATE kv_core)

# Benchmark binary
if(KV_BUILD_BENCH)
  add_executable(kv_bench src/bench/bench_main.cpp)
  target_link_libraries(kv_bench PRIVATE kv_core)
endif()

enable_testing()
add_executable(kv_tests
  tests/test_main.cpp
  tests/test_util.cpp
  tests/test_differential.cpp
  tests/test_wal_recovery.cpp
  tests/test_crash_recovery.cpp
  tests/test_iouring.cpp
  tests/test_backends.cpp
  tests/test_resp.cpp
  tests/test_raft.cpp
  tests/test_server.cpp
  tests/test_engine_features.cpp
  tests/test_concurrency.cpp
  tests/test_raft_tcp.cpp
  tests/test_remediation.cpp
)
target_link_libraries(kv_tests PRIVATE kv_core doctest::doctest)

include(${doctest_SOURCE_DIR}/scripts/cmake/doctest.cmake)
doctest_discover_tests(kv_tests)
