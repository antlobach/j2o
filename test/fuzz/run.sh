#!/usr/bin/env bash
set -euo pipefail
if [ "${GITHUB_ACTIONS:-}" != true ] || [ "${RUNNER_OS:-}" != Linux ]; then
  echo 'Fuzz qualification runs on Linux GitHub runners only' >&2; exit 1
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${H2O_SOURCE_DIR:?}" "${H2O_BUILD_DIR:?}"
mkdir -p "$root/.cache/fuzz/corpus" "$root/.cache/fuzz/artifacts"
# Deterministic seeds cover both host families, rollback, timeouts and loop failure.
printf '\001\000\000\000' > "$root/.cache/fuzz/corpus/ipv4"
printf '\001\001\001\001\002\003' > "$root/.cache/fuzz/corpus/localhost"
printf '\001\077\002\377\000\001' > "$root/.cache/fuzz/corpus/ipv6"
printf '\001\030\014\003\002\001\000' > "$root/.cache/fuzz/corpus/loop-error"
printf '\000\001\003\377\000\001' > "$root/.cache/fuzz/corpus/allocation-error"
"${CC:-clang}" -std=c11 -D_GNU_SOURCE -DH2O_USE_LIBUV=0 -Wall -Wextra -Werror \
  -O1 -g -fno-omit-frame-pointer -fsanitize=fuzzer,address,undefined \
  -isystem "$H2O_SOURCE_DIR/include" -isystem "$H2O_SOURCE_DIR/deps/picotls/include" \
  -isystem "$H2O_SOURCE_DIR/deps/quicly/include" \
  "$root/test/fuzz/lifecycle.c" -L"$H2O_BUILD_DIR" -lh2o-evloop -Wl,-rpath,"$H2O_BUILD_DIR" \
  -lssl -lcrypto -lz -lpthread -lm -ldl -o "$root/.cache/fuzz/lifecycle"
export ASAN_OPTIONS=detect_leaks=1:halt_on_error=1:abort_on_error=1
export LSAN_OPTIONS=exitcode=23:report_objects=1
export UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
"$root/.cache/fuzz/lifecycle" "$root/.cache/fuzz/corpus" -seed=20260306 \
  -max_total_time=180 -timeout=20 -max_len=96 -rss_limit_mb=2048 \
  -artifact_prefix="$root/.cache/fuzz/artifacts/" -print_final_stats=1
