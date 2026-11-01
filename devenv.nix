{ pkgs, lib, ... }: {
  packages = with pkgs; [
    gcc16
    clang_22
    cmake
    ninja
    pkg-config
    liburing
    openssl
    python3
    python3Packages.matplotlib
    perf
    linuxPackages.perf
    redis  # for redis-cli / redis-benchmark testing
  ];

  env = {
    CC = "gcc";
    CXX = "g++";
  };

  scripts = {
    kv-build.exec = ''
      cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo
      cmake --build build
    '';
    kv-test.exec = ''
      cd build && ctest --output-on-failure
    '';
  };

  enterShell = ''
    echo "kernel-v2 dev environment"
    echo "  gcc: $(gcc --version | head -1)"
    echo "  cmake: $(cmake --version | head -1)"
    echo "  liburing: $(pkg-config --modversion liburing)"
  '';
}
