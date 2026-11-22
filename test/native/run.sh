#!/usr/bin/env bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
revision=d3e43fcf00c3e6b17826b95d2d362d646185918a
dependency_dir="${J2O_DEPENDENCY_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/j2o}/h2o-$revision"
source_dir="${H2O_SOURCE_DIR:-$dependency_dir/source}"
build_dir="${H2O_BUILD_DIR:-$dependency_dir/build}"
case "$(uname -s)" in
  Darwin) openssl_root=${OPENSSL_ROOT_DIR:-$(brew --prefix openssl@3)}; extension=dylib ;;
  Linux) openssl_root=${OPENSSL_ROOT_DIR:-$(pkg-config --variable=prefix openssl)}; extension=so ;;
  *) echo "Native qualification requires Linux or macOS" >&2; exit 1 ;;
esac
bash "$root/script/build-native.sh"
flags=(-O2)
if [ -n "${J2O_SANITIZER:-}" ]; then
  flags=(-O1 -g -fno-omit-frame-pointer "-fsanitize=$J2O_SANITIZER")
fi
if [ "${J2O_LEAK_CHECK:-0}" = 1 ]; then
  export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1
  export LSAN_OPTIONS=exitcode=23:report_objects=1
  export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
fi
common=(-std=c11 -D_GNU_SOURCE -Wall -Wextra -Werror -DH2O_USE_LIBUV=0 "${flags[@]}")
"${CC:-cc}" "${common[@]}" -isystem "$source_dir/include" \
  -isystem "$source_dir/deps/picotls/include" -isystem "$source_dir/deps/quicly/include" \
  -isystem "$openssl_root/include" "$root/test/native/lifecycle.c" \
  -L"$build_dir" -lh2o-evloop -Wl,-rpath,"$build_dir" \
  -L"$openssl_root/lib" -lssl -lcrypto -lz -lpthread -lm -o "$root/.cache/native/lifecycle"
"${CC:-cc}" "${common[@]}" "$root/test/native/abi.c" \
  -L"$root/.cache/native" -lj2o_bridge -Wl,-rpath,"$root/.cache/native" \
  -o "$root/.cache/native/abi"
"${CC:-cc}" "${common[@]}" "$root/test/native/unary.c" \
  -L"$root/.cache/native" -lj2o_bridge -Wl,-rpath,"$root/.cache/native" \
  -o "$root/.cache/native/unary"
"$root/.cache/native/abi"
"$root/.cache/native/lifecycle"
"$root/.cache/native/unary"
