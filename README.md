# j2o

An H2O-backed Ring adapter under development for Jolt.

The repository contains executable transport experiments and a development unary Ring adapter. It is not production-ready or a complete `ring-chez` replacement: streaming, TLS, WebSockets, mTLS, and namespace parity remain unfinished. The transport uses real H2O connections.

## Constraints

- Target handler scheduling: a server-owned executor for the default and `:threads` modes, also used by `:fibers` unless `:handler-on-fiber true` explicitly selects direct request-fiber execution. The existing transport experiments use the earlier direct-fiber prototype.
- Collect-safe FFI permits GC but still occupies the calling carrier. Adapter-owned blocking work must not run on request carriers.
- Functions and Vars are supported. A Var is resolved at handler invocation, allowing live redefinition; already-running invocations keep their entry function.
- H2O is an **external shared-library dependency**. Its source/build cache is outside this repository, and it is not statically bundled into the bridge.
- Calls that can allocate, lock, copy variable amounts of data, or wait use collect-safe FFI transitions. C retains only native-owned buffers, never pointers into movable Jolt objects. Native H2O threads never call into Jolt.
- Performance measurements run **only on GitHub runners**, against unmodified H2O from the same pinned revision.

## Development native bridge

`native/j2o_bridge.h` defines exact-match development ABI `0x80000002`; it is not release ABI v1. `src/j2o/native.clj` checks the version before binding the remaining functions. The bridge accepts HTTP/1 and HTTP/2 unary requests and transfers owned request events and response commands across the H2O owner-thread boundary.

Start copies configuration and publishes a handle only after all resolved listeners and the H2O owner thread are initialized. Returned failures leave no server handle and roll back bridge-controlled resources. Stock H2O's process-fatal allocation, descriptor-creation and internal-notification failures remain process-fatal; the bridge does not intercept `h2o_fatal` or promise recovery from a host abort.

Errors are bounded caller-owned copies. A join timeout retains the server; successful join does not destroy it. The event descriptor is borrowed until destruction and must not be closed by callers. Destroy requires successful join and quiescent foreign callers.

The `Native lifecycle` GitHub workflow builds external H2O and the bridge with matching ASan/UBSan/leak or separate TSan instrumentation. It runs startup rollback, concurrent/repeated lifecycle, unary wire checks, real-join GC controls and stateful native fuzzing. CBMC checks production configuration/error functions with explicit libc models and bounded inputs; it does not prove H2O, OS behavior or lifecycle concurrency. Allium structural/process checks are specification evidence, not a proof of C implementation correctness.

## Reproduce correctness experiments
Run the commands below in GitHub Actions, not on the development workstation. Builds, correctness suites, sanitizers, GC probes, stress, and performance workloads are remote-only for this project.


Requires Jolt **v0.8.28**, Git, a C11 compiler, CMake, pkg-config, OpenSSL 3, zlib, and an HTTP/2-enabled curl. macOS uses Homebrew `openssl@3`; Linux uses the installed development libraries. `OPENSSL_ROOT_DIR` can override discovery.

```sh
bash experiments/build.sh
jolt -m j2o.experiments.fibers
jolt -m j2o.experiments.gc safe
jolt -m j2o.experiments.gc unsafe
jolt -m j2o.experiments.differential
jolt -m j2o.experiments.http2
jolt -m j2o.experiments.profile-checks
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
- Optional bounded profiling records: request identity across concurrent fibers, cumulative allocation across collections, recording-cap overflow without HTTP admission loss, and incomplete deadline traces excluded from successful captures.

H2O's protocol behavior is the baseline; matching the older adapter's rejection policy is not a requirement. The selected revision accepts some framing/line-ending inputs that `ring-chez-adapter` rejects. Wire-level safety gates concern correct message boundaries, required connection closure, and isolation—not identical rejection. Permissible leniency must still be distinguished from genuinely ambiguous framing.

## Performance

PR and main-branch updates run correctness and safety checks, **not benchmarks**. Performance runs are manual-only at stable milestones: dispatch `Experiments` with `benchmarks=true` for bridge benchmarks/profiles, or dispatch `Native H2O versus ring-chez` for the direct stock-server comparison. The former runs correctness gates before measuring and saves environment metadata, raw wrk output, throughput, p50/p95/p99 latency, thread counts, and RSS. Performance workloads refuse to run outside Linux GitHub Actions.

The native static-file server is a transport ceiling, not an equivalent Ring application. Shared-runner results characterize this experiment; they are not a production performance guarantee.

The workflow also profiles single-request latency in fresh server processes with profiling off, sampled stage timing, sampled allocation counters, and CPU sampling. GC stays enabled in every mode. It records observer calibration and an idle allocation control. Timing and allocation collection are separate from the CPU-sampling run; all modes retain response validation. Stage records are native-owned, capped, and exported only after completion. Queue intervals include wakeup and scheduling; `h2o_send_inline` returning does not mean bytes have reached the client.

Profiling preserves both load-generator completions and server admissions: wrk can stop with one unreported reply per non-pipelined connection. Windows wait for closed connections and settled native/fiber work; only a final transport-stale trace may be incomplete. Allocation snapshots combine live and reclaimed bytes within one GC epoch, and process CPU is reported separately from the controller thread's CPU.

The paired ingress investigation separates HTTP qualification windows (five unique rounds at concurrency 1/16/64) from profiling windows (three unique rounds per variant/mode at concurrency 1, each round including profiling off). The reducer rejects missing/duplicated windows, failed response validation, missing captures, and retained native work. It exports raw paired ratios, throughput/p99 win counts, and exact deterministic paired-bootstrap 95% intervals over all \(5^5\) paired resamples.

CPU sampling starts immediately before measured load and stops before quiescence/export. Timing, allocation, CPU, and observer calibration remain observer-inclusive; calibration is not subtracted and the native throughput gap is not classified as removable overhead. Response preparation precedes terminal arbitration; its measured preparation-to-submit interval includes the native allocation/copy, managed arbitration, and submission handoff.

Experimental ingress preserves one notification per queued request. Prepared responses transfer ownership only on successful submission, losing producers release their commands, and startup failures retire waiters/fibers before native teardown. Child-process deadlines escalate through timed wait, termination, a second timed wait, and forced termination. Root failures survive cleanup errors; because Jolt does not support adding suppressed exceptions, secondary cleanup failures are attached in `ExceptionInfo` data with the original failure as cause.

Measured runner results and the implementation sequence are in [PLAN.md](PLAN.md#runner-baseline-2026-11-21), including the [verified single-request profile](PLAN.md#single-request-profile-verified-runner-results). Raw samples, environment metadata, and load-generator output are attached to the linked GitHub runs.
