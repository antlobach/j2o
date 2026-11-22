#!/usr/bin/env bash
set -euo pipefail
if [ "${GITHUB_ACTIONS:-}" != true ] || [ "${RUNNER_OS:-}" != Linux ]; then
  echo 'GC qualification runs on Linux GitHub runners only' >&2; exit 1
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${H2O_SOURCE_DIR:?}" "${H2O_BUILD_DIR:?}"
"${CC:-clang}" -std=c11 -D_GNU_SOURCE -DH2O_USE_LIBUV=0 -Wall -Wextra -Werror \
  -O2 -fPIC -fvisibility=hidden -shared -Wl,-z,defs \
  -isystem "$H2O_SOURCE_DIR/include" -isystem "$H2O_SOURCE_DIR/deps/picotls/include" \
  -isystem "$H2O_SOURCE_DIR/deps/quicly/include" \
  "$root/test/native/gc.c" -L"$H2O_BUILD_DIR" -lh2o-evloop -Wl,-rpath,"$H2O_BUILD_DIR" \
  -lssl -lcrypto -lz -lpthread -lm -ldl -o "$root/.cache/native/libj2o_gc.so"
export J2O_BRIDGE_LIBRARY="$root/.cache/native/libj2o_gc.so"
jolt -M:gc safe
jolt -M:gc unsafe
