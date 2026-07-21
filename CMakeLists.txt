cmake_minimum_required(VERSION 3.28)
project(kernel_v2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Compatibility for FetchContent deps with old cmake_minimum_required
set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING "" FORCE)

# Options
option(KV_BUILD_BENCH "Build benchmarks" ON)
option(KV_USE_IOURING "Enable io_uring backend" ON)

# Find system liburing
if(KV_USE_IOURING)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing>=2.15)
endif()

# doctest via FetchContent
include(FetchContent)
FetchContent_Declare(
  doctest
  GIT_REPOSITORY https://github.com/doctest/doctest.git
  GIT_TAG        v2.4.11
  GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(doctest)

# Main library (header-only for now, will grow)
add_library(kv_core INTERFACE)
target_include_directories(kv_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_features(kv_core INTERFACE cxx_std_23)

if(KV_USE_IOURING)
  target_link_libraries(kv_core INTERFACE PkgConfig::LIBURING)
  target_compile_definitions(kv_core INTERFACE KV_HAS_IOURING=1)
endif()

# Main binary (placeholder)
add_executable(kv_server src/main.cpp)
target_link_libraries(kv_server PRIVATE kv_core)

# Tests
enable_testing()
add_executable(kv_tests
  tests/test_main.cpp
  tests/test_hello.cpp
)
target_link_libraries(kv_tests PRIVATE kv_core doctest::doctest)

include(${doctest_SOURCE_DIR}/scripts/cmake/doctest.cmake)
doctest_discover_tests(kv_tests)
