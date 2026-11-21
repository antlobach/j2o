#!/usr/bin/env bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
revision=d3e43fcf00c3e6b17826b95d2d362d646185918a
# H2O is an external dependency, NEVER copied into this project or our bridge.
dependency_dir="${J2O_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/j2o}/h2o-$revision"
source_dir="${H2O_SOURCE_DIR:-$dependency_dir/source}"
build_dir="${H2O_BUILD_DIR:-$dependency_dir/build}"
mkdir -p "$root/.cache" "$dependency_dir"
if [ ! -d "$source_dir/.git" ]; then
  git init "$source_dir"
  git -C "$source_dir" remote add origin https://github.com/h2o/h2o.git
  git -C "$source_dir" fetch --depth 1 origin "$revision"
  git -C "$source_dir" checkout --detach FETCH_HEAD
fi
if [ "$(git -C "$source_dir" rev-parse HEAD)" != "$revision" ]; then
  echo "Expected external H2O revision $revision in $source_dir" >&2
  exit 1
fi
cmake_options=()
link_options=(-lz -lpthread -lm)
case "$(uname -s)" in
  Darwin)
    openssl_root=${OPENSSL_ROOT_DIR:-$(brew --prefix openssl@3)}
    cmake_options+=("-DOPENSSL_ROOT_DIR=$openssl_root"
      "-DOPENSSL_INCLUDE_DIR=$openssl_root/include"
      "-DOPENSSL_SSL_LIBRARY=$openssl_root/lib/libssl.dylib"
      "-DOPENSSL_CRYPTO_LIBRARY=$openssl_root/lib/libcrypto.dylib")
    output="$root/.cache/libj2o_probe.dylib"
    ;;
  Linux)
    openssl_root=${OPENSSL_ROOT_DIR:-$(pkg-config --variable=prefix openssl)}
    cmake_options+=("-DOPENSSL_ROOT_DIR=$openssl_root")
    link_options+=(-ldl)
    output="$root/.cache/libj2o_probe.so"
    ;;
  *) echo "This experiment currently builds on macOS or Linux only" >&2; exit 1 ;;
esac
cmake -S "$source_dir" -B "$build_dir" \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_WITH_INSTALL_NAME_DIR=ON \
  "-DCMAKE_INSTALL_NAME_DIR=$build_dir" \
  -DWITH_MRUBY=OFF -DWITH_DTRACE=OFF -DWITH_FUSION=OFF \
  -DWITH_BROTLI=OFF -DWITH_ZSTD=OFF -DDISABLE_LIBUV=ON \
  "${cmake_options[@]}"
cmake --build "$build_dir" --target libh2o-evloop h2o --parallel "${J2O_BUILD_JOBS:-4}"
"${CC:-cc}" -std=c11 -D_GNU_SOURCE -O2 -Wall -Wextra -Werror -fPIC -shared \
  -DH2O_USE_LIBUV=0 -isystem "$source_dir/include" \
  -isystem "$source_dir/deps/picotls/include" -isystem "$source_dir/deps/quicly/include" \
  -isystem "$openssl_root/include" "$root/experiments/bridge.c" \
  -L"$build_dir" -lh2o-evloop -Wl,-rpath,"$build_dir" \
  -L"$openssl_root/lib" -lssl -lcrypto "${link_options[@]}" -o "$output"
printf 'Built %s, dynamically linked to external H2O %s\n' "$output" "$revision"
printf '%s\n' "$build_dir/h2o" > "$root/.cache/h2o-reference-path"
