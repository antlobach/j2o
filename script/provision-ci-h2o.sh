#!/usr/bin/env bash
set -euo pipefail
if [ "${GITHUB_ACTIONS:-}" != true ] || [ "${RUNNER_OS:-}" != Linux ]; then
  echo "Dependency provisioning and qualification run on Linux GitHub runners only" >&2
  exit 1
fi
: "${H2O_SOURCE_DIR:?Provide an external source directory}"
: "${H2O_BUILD_DIR:?Provide an external build directory}"
revision=d3e43fcf00c3e6b17826b95d2d362d646185918a
if [ ! -d "$H2O_SOURCE_DIR/.git" ]; then
  git init "$H2O_SOURCE_DIR"
  git -C "$H2O_SOURCE_DIR" remote add origin https://github.com/h2o/h2o.git
  git -C "$H2O_SOURCE_DIR" fetch --depth 1 origin "$revision"
  git -C "$H2O_SOURCE_DIR" checkout --detach FETCH_HEAD
fi
test "$(git -C "$H2O_SOURCE_DIR" rev-parse HEAD)" = "$revision"
flags=()
if [ -n "${J2O_SANITIZER:-}" ]; then
  sanitizer="-fsanitize=$J2O_SANITIZER -fno-omit-frame-pointer"
  flags+=("-DCMAKE_C_FLAGS=$sanitizer" "-DCMAKE_CXX_FLAGS=$sanitizer"
          "-DCMAKE_SHARED_LINKER_FLAGS=$sanitizer" "-DCMAKE_EXE_LINKER_FLAGS=$sanitizer")
fi
# Never run this from the repository: upstream configure probes may emit headers.
mkdir -p "$H2O_BUILD_DIR"
cmake -S "$H2O_SOURCE_DIR" -B "$H2O_BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_FLAGS_DEBUG='-O1 -g' -DCMAKE_CXX_FLAGS_DEBUG='-O1 -g' \
  -DBUILD_SHARED_LIBS=ON -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DWITH_MRUBY=OFF -DWITH_DTRACE=OFF -DWITH_FUSION=OFF -DWITH_BROTLI=OFF -DWITH_ZSTD=OFF \
  -DDISABLE_LIBUV=ON "${flags[@]}"
cmake --build "$H2O_BUILD_DIR" --target libh2o-evloop --parallel 4
