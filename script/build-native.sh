#!/usr/bin/env bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
revision=d3e43fcf00c3e6b17826b95d2d362d646185918a
dependency_dir="${J2O_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/j2o}/h2o-$revision"
source_dir="${H2O_SOURCE_DIR:-$dependency_dir/source}"
build_dir="${H2O_BUILD_DIR:-$dependency_dir/build}"
# Consume an already-provisioned external H2O. Never fetch or build it here.
if [ ! -f "$source_dir/include/h2o.h" ] || [ ! -d "$build_dir" ]; then
  echo "Provide external H2O $revision using H2O_SOURCE_DIR and H2O_BUILD_DIR" >&2
  exit 1
fi
if [ "$(git -C "$source_dir" rev-parse HEAD)" != "$revision" ]; then
  echo "Unsupported H2O revision: expected $revision" >&2
  exit 1
fi
links=(-lz -lpthread -lm)
case "$(uname -s)" in
  Darwin)
    openssl_root=${OPENSSL_ROOT_DIR:-$(brew --prefix openssl@3)}
    extension=dylib
    ;;
  Linux)
    openssl_root=${OPENSSL_ROOT_DIR:-$(pkg-config --variable=prefix openssl)}
    extension=so
    links+=(-ldl)
    if [ -z "${J2O_SANITIZER:-}" ]; then links+=(-Wl,-z,defs); fi
    ;;
  *) echo "j2o currently supports Linux and macOS builds" >&2; exit 1 ;;
esac
if [ ! -f "$build_dir/libh2o-evloop.$extension" ]; then
  echo "Missing external shared libh2o-evloop in $build_dir" >&2
  exit 1
fi
mkdir -p "$root/.cache/native"
flags=(-O2)
if [ -n "${J2O_SANITIZER:-}" ]; then
  flags=(-O1 -g -fno-omit-frame-pointer "-fsanitize=$J2O_SANITIZER")
fi
"${CC:-cc}" -std=c11 -D_GNU_SOURCE -Wall -Wextra -Werror -fPIC -fvisibility=hidden -shared \
  -DH2O_USE_LIBUV=0 "${flags[@]}" \
  -isystem "$source_dir/include" -isystem "$source_dir/deps/picotls/include" \
  -isystem "$source_dir/deps/quicly/include" -isystem "$openssl_root/include" \
  "$root/native/j2o_bridge.c" -L"$build_dir" -lh2o-evloop -Wl,-rpath,"$build_dir" \
  -L"$openssl_root/lib" -lssl -lcrypto "${links[@]}" \
  -o "$root/.cache/native/libj2o_bridge.$extension"
printf 'Built development ABI 0x80000002 against external H2O %s\n' "$revision"
