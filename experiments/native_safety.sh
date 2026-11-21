#!/usr/bin/env bash
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
read -r reference < "$root/.cache/h2o-reference-path"
build_dir=$(dirname -- "$reference")
source_dir="${H2O_SOURCE_DIR:-$(dirname -- "$build_dir")/source}"
case "$(uname -s)" in
  Darwin) openssl_root=${OPENSSL_ROOT_DIR:-$(brew --prefix openssl@3)} ;;
  Linux) openssl_root=${OPENSSL_ROOT_DIR:-$(pkg-config --variable=prefix openssl)} ;;
  *) echo "Native safety probe supports macOS/Linux" >&2; exit 1 ;;
esac
"${CC:-cc}" -std=c11 -D_GNU_SOURCE -O1 -g -Wall -Wextra -Werror \
  -fsanitize=address,undefined -fno-omit-frame-pointer -DH2O_USE_LIBUV=0 \
  -isystem "$source_dir/include" -isystem "$source_dir/deps/picotls/include" \
  -isystem "$source_dir/deps/quicly/include" -isystem "$openssl_root/include" \
  "$root/experiments/native_safety.c" -L"$build_dir" -lh2o-evloop \
  -Wl,-rpath,"$build_dir" -L"$openssl_root/lib" -lssl -lcrypto -lz -lpthread -lm \
  -o "$root/.cache/native-safety"
"$root/.cache/native-safety"
