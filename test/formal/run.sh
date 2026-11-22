#!/usr/bin/env bash
set -euo pipefail
if [ "${GITHUB_ACTIONS:-}" != true ] || [ "${RUNNER_OS:-}" != Linux ]; then
  echo 'Bounded verification runs on Linux GitHub runners only' >&2; exit 1
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
: "${H2O_SOURCE_DIR:?}"
mkdir -p "$root/.cache/formal"
cbmc --version
# Actual production translation unit, sliced by entry point. No lifecycle or
# H2O concurrency claim: these harnesses cover config setters and copied errors.
status=0
for entry in verify_numeric verify_host verify_errors; do
  if timeout 240 cbmc "$root/test/formal/config.c" --function "$entry" \
    -D_GNU_SOURCE -DH2O_USE_LIBUV=0 \
    -I "$H2O_SOURCE_DIR/include" -I "$H2O_SOURCE_DIR/deps/picotls/include" \
    -I "$H2O_SOURCE_DIR/deps/quicly/include" \
    --bounds-check --pointer-check --pointer-overflow-check --signed-overflow-check \
    --memory-leak-check --malloc-may-fail --malloc-fail-null \
    --unwind 600 --unwinding-assertions --slice-formula --trace \
    2>&1 | tee "$root/.cache/formal/$entry.log"; then
    :
  else
    status=1
  fi
done
exit "$status"
