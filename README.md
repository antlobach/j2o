# j2o

A fiber-first Ring adapter for Jolt, using external H2O for HTTP transport.

This repository currently contains executable design experiments and an implementation plan, not a production adapter or a published public API. The experiments exercise real H2O connections and real Jolt fibers; there is no substitute HTTP server or simulated transport.

## Constraints

- Synchronous, one-argument Ring handlers run on Jolt fibers.
- Applications explicitly offload carrier-blocking native calls; the adapter does not offload handlers automatically.
- Functions and Vars are supported. A Var is resolved at handler invocation, allowing live redefinition; already-running invocations keep their entry function.
- H2O is an **external shared-library dependency**. Its source/build cache is outside this repository, and it is not statically bundled into the bridge.
- Calls that can allocate, lock, copy variable amounts of data, or wait use collect-safe FFI transitions. C retains only native-owned buffers, never pointers into movable Jolt objects. Native H2O threads never call into Jolt.
- Performance measurements run **only on GitHub runners**, against unmodified H2O from the same pinned revision.

## Reproduce correctness experiments

Requires Jolt **v0.8.27**, Git, a C11 compiler, CMake, pkg-config, OpenSSL 3, zlib, and an HTTP/2-enabled curl. macOS uses Homebrew `openssl@3`; Linux uses the installed development libraries. `OPENSSL_ROOT_DIR` can override discovery.

```sh
bash experiments/build.sh
jolt -m j2o.experiments.fibers
jolt -m j2o.experiments.gc safe
jolt -m j2o.experiments.gc unsafe
jolt -m j2o.experiments.differential
jolt -m j2o.experiments.http2
bash experiments/native_safety.sh
```

The build script fetches pinned H2O into `${XDG_CACHE_HOME:-$HOME/.cache}/j2o/`, builds the external shared library and its unmodified standalone server, and builds only the j2o experimental bridge into `.cache/`. `J2O_DEPENDENCY_CACHE`, `H2O_SOURCE_DIR`, and `H2O_BUILD_DIR` override dependency paths. No H2O source or binary belongs in project commits.

The `unsafe` GC probe is an intentional negative control, isolated in its own process: collection must wait until an unmarked native call returns. The `safe` probe must observe collection **before** the corresponding native call returns. Neither path passes managed heap pointers to C.

## What is exercised

- One carrier serving many parked fibers; explicit native-thread offload; dynamic bindings; Var redefinition; handler replacement.
- Fiber-local cancellation that bypasses catch-all code and runs `finally`, including a computing fiber.
- Actual HTTP/1.1 and HTTP/2, keep-alive/pipelining, binary bodies, fragmented chunked uploads, bounded admission, handler errors, deadline cleanup, disconnects, late responses, and repeated lifecycle teardown.
- Native H2O comparisons for GET/HEAD, pipelining, and selected framing/parser cases.
- Explicit collections during native waits and concurrent HTTP traffic.
- ASan/UBSan on the bridge's native binary boundaries and teardown paths. H2O itself is an external, uninstrumented dependency in this sanitizer experiment.

Matching H2O is not proof of stricter Ring compatibility: the selected native revision accepts some framing/line-ending inputs that `ring-chez-adapter` rejects. This is a production-readiness and compatibility gate, not a claim that the experimental bridge is hardened.

## Performance

The `Experiments` GitHub Actions workflow runs correctness gates first, then benchmarks unmodified H2O and the experimental Jolt bridge with validated identical response bytes. It saves environment metadata, raw wrk output, throughput, p50/p95/p99 latency, thread counts, and RSS. The benchmark refuses to run outside Linux GitHub Actions.

The native static-file server is a transport ceiling, not an equivalent Ring application. Shared-runner results characterize this experiment; they are not a production performance guarantee.
