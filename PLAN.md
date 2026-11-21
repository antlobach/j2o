# j2o implementation plan

## Target contract

An embedded H2O Ring adapter for Jolt, with synchronous one-argument handlers running on fibers. Application code explicitly offloads carrier-blocking work. Functions and Vars are accepted; resolving a Var happens once at the beginning of an invocation. Handler replacement affects subsequent invocations, including on existing connections. No async Ring callback arity, automatic handler executor, or direct-on-H2O handler mode.

H2O remains an external, dynamically linked dependency. Do not copy H2O source or binaries into j2o, and do not statically bundle H2O into the bridge. The experiment builds an unmodified pinned revision in an external cache only to make its observations reproducible. Production packaging should consume a supported external H2O development/runtime package and verify the ABI at startup.

## Evidence already obtained

Environment: Jolt v0.8.27; external H2O revision `d3e43fcf00c3e6b17826b95d2d362d646185918a` (library 0.16); OpenSSL 3. Local execution is correctness/feasibility only. Performance measurements belong to GitHub runners.

| Experiment | Observed behavior | Design consequence |
|---|---|---|
| Single-carrier fibers | 128 fibers complete waits; another fiber runs while a synchronous helper waits | Use `jolt.fibers/spawn` directly; do not depend on core.async's default `go` backend, which is threads |
| Native FFI control | An explicitly collect-safe native sleep still holds its carrier | GC-safe does not mean fiber-safe; applications must explicitly offload blocking native work |
| Explicit offload | A native-thread operation does not hold the request carrier; an error envelope propagates failure | Document the offload pattern without automatic detection or hidden executors |
| Var and swap behavior | Var roots update on subsequent invocations; captured function values retain the old entry; an existing invocation retains its selected entry | Store the supplied target, resolve once in the request fiber, never cache a Var root at startup |
| Dynamic bindings | Spawned fibers inherit caller bindings; the parent binding remains unchanged | Establish explicit request bindings when dispatching; do not accidentally inherit startup/REPL bindings as request state |
| Cancellation | `kill!` bypasses catch-all code, runs `finally`, wakes a channel waiter without consuming its next value, and stops a computing fiber | Handler deadlines are feasible on this runtime without native-thread handler execution |
| Actual embedded HTTP | H2O callback dispatches into Jolt; a synchronous handler observes `in-fiber? = true`; response crosses back to the owning loop | Keep H2O pointers loop-confined and exchange opaque IDs plus owned bytes |
| HTTP/1 differential | GET, HEAD metadata/body suppression, and pipelining match unmodified standalone H2O | Reuse H2O framing, with additional Ring response policy explicitly verified |
| HTTP/2 | curl prior-knowledge HTTP/2 succeeds against both servers; the handler sees HTTP/2 | Basic H2 embedding works; multiplexing, flow control, resets, and TLS/ALPN still require dedicated gates |
| Request handling | Binary round-trip and fragmented chunked upload succeed; body cap rejects oversized input | Expose a byte-faithful InputStream over owned bounded request bytes initially |
| Admission | 64 parked handlers coexist on one carrier; an extra request gets 503; admitted requests finish after release | Bound pending work independently of carrier count; make production limits configurable |
| Errors/reload | Throw, nil response, invalid body, and invalid Var root fail the request; subsequent requests on the same connection recover | Keep request failures inside the request boundary, with one `:on-failure` attempt |
| Response validation | A response over the experimental native byte limit now receives 500 instead of leaving the request unanswered | Encode and validate response bytes before claiming the terminal response state |
| Lifetime | Unknown/late IDs do not corrupt a following response; disconnect/release succeeds; stop runs cleanup and closes the port; repeated starts work | Native request destruction must invalidate IDs; shutdown must join all users before freeing bridge state |
| GC controls | Explicit collection completes while collect-safe native work is still running, but not during the corresponding unmarked call | Preserve collect-safe transitions on every potentially long native call |
| GC under traffic | Explicit collections complete while concurrent real HTTP requests succeed | Keep collection-progress checks in CI, not just a no-crash assertion |
| Native sanitizers | ASan/UBSan probe exercises zero/one/max binary bodies, oversized responses, stale replies, and pending teardown over ten server lifecycles | Extend these boundary/lifetime scenarios as the ABI grows; the external H2O library is not instrumented by this probe |
| Thread-cache lifetime | Linux LeakSanitizer detected caches surviving loop-thread exit in the initial embedding | Run H2O's periodic cache maintenance and explicitly release its thread-local recyclers before the owning thread exits; keep leak detection enabled |

## Important compatibility finding

The selected H2O revision returns 200 for the exercised bare-LF, conflicting Content-Length, and Content-Length plus chunked Transfer-Encoding requests. These cases have different requirements: [RFC 9112 §2.2](https://www.rfc-editor.org/rfc/rfc9112.html#section-2.2) permits single-LF parsing; [§6.1](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.1) permits processing Transfer-Encoding with mandatory connection closure; [§6.3](https://www.rfc-editor.org/rfc/rfc9112.html#section-6.3) requires rejecting conflicting Content-Length values without Transfer-Encoding. H2O's source forces closure for the mixed-header case. Acceptance of conflicting lengths in this revision needs standards-compliance investigation; matching the reference alone does not certify correct framing.

Protocol behavior follows supported H2O versions. Compatibility tests verify standards-required behavior, message boundaries, connection closure, and request isolation rather than identical rejection policies. Permitted leniency and ambiguous framing require separate analysis. H2O remains an external dependency; j2o does not add a second HTTP parser or maintain a private H2O fork.

## Native and GC invariants

Reference: [jjson's collect-safe design](https://github.com/antlobach/jjson/blob/main/src/jjson.clj) and [its native transition rules](https://github.com/antlobach/jjson/blob/main/native/jjson_chez.c).

1. Native H2O threads never call Scheme/Jolt and never own managed references.
2. Request pointers and H2O pools are accessed only on their owning event loop.
3. Native waits, joins, locks, variable-size copies, allocation, and destruction use collect-safe FFI transitions. Only bounded scalar/field accessors remain unmarked.
4. The current ABI transfers only native allocations. No pointer to movable managed bytes may outlive a call. Any later zero-copy design must pin/lock the exact object, retain it for the entire native lifetime, and release it on every completion/cancellation/error path. Being collect-safe alone is not sufficient.
5. Deactivated C code must not allocate Scheme objects or call Scheme.
6. Wakeup descriptors are nonblocking. Jolt waits through its readiness poller, not a blocking FFI wait on a carrier. Cancellation and poller deregistration precede descriptor close/reuse.
7. Exactly one terminal outcome wins per request. A deadline, disconnect, handler completion, and shutdown must not each submit a response.
8. Native destruction cannot occur until dispatch, request fibers, deadline supervisors, and all native callers have finished. A timeout while joining is an error, not permission to free live state.

## Production implementation sequence

### 1. Establish the supported external dependency and ABI

Select supported H2O/OpenSSL package versions and platforms. Verify the parser finding above. Ship a small j2o-owned bridge with opaque server/request/buffer handles, explicit ownership/result codes, ABI version checks, and no H2O struct layouts in Clojure.

Keep dependency install/build tooling separate from runtime startup. Missing/incompatible dependencies should fail with actionable diagnostics before binding listeners. The experimental automatic source build is not the settled production installer.

Acceptance: clean dynamic linkage, supported-version matrix, byte/length boundaries under sanitizers, startup rollback with no leaked fd/thread, and verified package use without any H2O content in the repository.

### 2. Complete request lifecycle and Ring mapping

Implement listener configuration, atomic IPv4/IPv6/hostname binding, unified port zero, optional reuse-port, complete Ring fields/headers, peer addresses, URI/raw-target policy, header/body limits, and arrival/idle/write deadlines.

Use per-loop generation-safe request IDs. Add native destruction/cancellation events back into Jolt, not only ID invalidation. Bound admitted requests, dispatch queues, queued response commands, connection counts, and total queued bytes independently. Snapshot handler targets at invocation.

Acceptance: same-connection redefinition, no cross-request byte leakage, overload behavior, deadlines versus completion races, cancellation before dispatch and after dispatch, startup/stop races, and failure recovery.

### 3. Finish body and long-lived-stream support

Add File, InputStream, sequence, and channel response bodies. File/stream reads supplied by the adapter must not block carriers. Pull body data only when transport capacity allows; enforce per-stream and server-wide byte budgets. Known lengths must match bytes actually sent; unknown lengths must use protocol-appropriate framing. HEAD/204/304/bodyless behavior needs independent cases.

SSE is a body policy over the same streaming path. An idle disconnected stream must cancel its producer without requiring another event to be emitted.

Acceptance: slow-reader backpressure, bounded memory, HTTP/2 stream isolation, stream failure after headers, idle disconnect, producer cancellation, and release of every native buffer on all terminal paths.

### 4. Complete lifecycle and failure API

Expose `j2o.adapter/run-server`, `stop-server`, `swap-handler!`, `swap-ws-handler!`, and `server-stats`. Functions and Vars have consistent semantics across the handler, WebSocket handler, guard, and failure callback.

Implement graceful draining and forced shutdown, not just the experiment's forced teardown. Distinguish application failures from server faults. Preserve useful default status behavior and failure metadata, but use j2o's namespace rather than compatibility aliases.

For handler deadlines, use verified fiber-local kill plus an atomic terminal claim. Cancellation remains cooperative at runtime safe points: a fiber blocked in arbitrary foreign code cannot be forcibly unwound until control returns. A deadline does not promise to terminate application-owned offload threads.

Acceptance: finally/catch behavior, compute-stuck and parked handlers, late completion after timeout, streaming duration independent of handler deadline, and shutdown with keep-alive/HTTP2/streaming work.

### 5. WebSockets and middleware parity

Use external H2O/wslay transport capabilities, subject to package availability. Preserve pre-upgrade guard behavior and a synchronous session API on a fiber. Verify message limits, UTF-8, masking, fragmentation, control frames, ping/pong, close codes, write limits, and disconnect cancellation. H2 WebSocket support is not implied by HTTP/1 upgrade support.

Port or extract transport-independent multipart, trusted-proxy, gzip/zstd, SSE, fault-response, and static-file behavior with attribution. Reuse existing dependencies such as the multipart parser and crypto support where appropriate. Preserve compression negotiation and streaming flush semantics; native compression defaults are not automatically the same middleware policy.

Acceptance: feature-by-feature compatibility cases derived from the reference adapter's documented external behavior, including multipart shapes, ETags, cache validators, traversal/symlink protection, proxy trust, and persistent compression contexts.

### 6. TLS, platform gates, and release qualification

Add TLS/mTLS certificate validation, client certificates, handshake bounds, and ALPN. Validate platforms individually rather than inferring Windows support from Jolt's portability. Run upstream/proven-client comparisons, sanitizers, allocation-failure coverage, and concurrency/lifecycle stress before declaring parity.

HTTP/3 is a separate scope decision, not required for ring-chez feature parity.

## Performance methodology

Only the GitHub `Experiments` workflow runs performance measurements. It first runs correctness, explicit GC positive/negative controls, real native-reference comparisons, HTTP/2, and native sanitizer checks.

Measure the external unmodified H2O static-file server versus a separate Jolt bridge process. Both return exactly `Hello, World!\n`. Validate status and body on every load-generator response. Save raw output, runner/CPU/kernel/compiler/runtime versions, commit, throughput, p50/p95/p99 latency, process thread count, and RSS. Use one H2O I/O thread, one j2o I/O thread plus one carrier, concurrency 1/16/64, warmup before every sample, three samples, and alternating server order.

Interpret native H2O as a transport ceiling, not an equivalent Ring stack or proof of a production speed claim. Do not optimize by skipping validation, truncating binary data, disabling GC safety, weakening limits, or dropping lifecycle cleanup. Profile the measured bridge cost before considering batching, buffer reuse, fewer FFI crossings, or pinned buffers. Every optimization must preserve the same correctness gates.

## Runner baseline: 2026-11-21

[Verified run and raw artifacts](https://github.com/antlobach/j2o/actions/runs/44929790959), commit `4856326185621b4b4806a18c221c239e3409fa9e`. All correctness, GC, HTTP/2, address/undefined-behavior/leak sanitizer, and benchmark gates passed.

Ubuntu 22.04 GitHub runner; four logical CPUs on Intel Xeon Platinum 8573C; Jolt v0.8.27; H2O 2.3.0-DEV at the pinned revision; OpenSSL 3.0.2; GCC 11.4; wrk 4.1.0. Each cell below is the median of three per-run measurements, not a percentile of pooled latency samples.

| Server | Concurrency | Requests/sec | p50 µs | p95 µs | p99 µs | OS threads | RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| Native H2O | 1 | 26,555 | 34 | 46 | 59 | 2 | 7.8 |
| j2o experiment | 1 | 7,714 | 124 | 154 | 188 | 5 | 117.9 |
| Native H2O | 16 | 101,703 | 129 | 187 | 212 | 2 | 7.9 |
| j2o experiment | 16 | 56,965 | 263 | 316 | 472 | 6 | 118.3 |
| Native H2O | 64 | 101,674 | 290 | 630 | 672 | 2 | 7.9 |
| j2o experiment | 64 | 73,101 | 814 | 970 | 1,098 | 6 | 118.8 |

All 18 measured samples reported zero response mismatches and zero load-generator failures, covering 3,368,087 measured requests. Warmup responses were validated as well. The j2o server completed 2,084,963 total requests including warmups, with matching answered/disposed counts, zero rejected or stale responses, zero live request packets at shutdown, and empty benchmark stderr.

The native static-file path does less application/runtime work and remains a ceiling, not a like-for-like Ring comparison. RSS includes the entire Jolt process. Thread counts are process snapshots during load, not a claim that every runtime background thread has been classified.

Single-request profiling uses fresh server processes for collection off, sampled timing, sampled allocation, CPU sampling, and an idle control. Three alternating-order rounds use concurrency one, a two-second validated warmup and three-second measured window. Timing and allocation modes select approximately one in eight request IDs with deterministic mixing and cap storage at 32,768 native records. The same carrier executes adjacent empty observer marks before the measured window for calibration.

All 14 stage timestamps use native `CLOCK_MONOTONIC`. Boundaries distinguish native snapshot preparation, ingress queue handoff, request decoding, fiber dispatch, handler invocation, response encoding, output-buffer marshalling, native submission, outbound queue handoff, and the H2O send call. Queue handoff includes wakeup and scheduling; the final timestamp is `h2o_send_inline` return, not wire flush. Their sum is a server-side interval, not the client's complete latency.

Allocation counters are process-wide cumulative Chez allocation, including runtime and observer work, not exclusive per-function allocation. The raw counter adds currently occupied and deallocated bytes, matching the cumulative calculation documented by [Chez Scheme](https://cisco.github.io/ChezScheme/csug10.0/system.html). Whole-window deltas and idle controls supplement sampled stage deltas. Known native byte counts cover only request packets and response commands, not all H2O allocation. CPU samples cover active Jolt threads, not the native H2O event-loop thread. The off control retains dormant hook branches, so enabling overhead is not a comparison against an older uninstrumented binary.

Live and reclaimed byte counters are read within one GC epoch; if collection occurs between those reads, the observer takes a fresh snapshot. Mixing pre-GC live bytes with post-GC reclaimed bytes overcounts allocation and can make a later cumulative reading decrease. Both sampled marks and whole-window allocation use this epoch-consistent reader without disabling GC.

Timed [wrk 4.1.0](https://github.com/wg/wrk/blob/4.1.0/src/wrk.c) stops its event loop without draining the final outstanding response. Recording windows therefore require zero active connections and settled native/fiber work at both boundaries. Report client-validated completions and server admissions, answers, and stale replies separately; normalize allocation by server admissions. The unreported tail must be bounded by one request per non-pipelined connection. An incomplete timing sample is allowed only for that final stale request with every stage through native response receipt present; unexplained incompleteness and recording overflow fail the run.

CPU scope is explicit: Chez statistics reports the calling thread's CPU time, while `time-process` measures all server process threads, including native H2O. Both counters are retained under distinct keys. Sampling describes captured Jolt CPU stacks, not total process CPU or wall time.

Profiling correctness checks exercise actual binary requests, explicit collection, same-connection Var redefinition, concurrent identities, bounded recording overflow, and deadline cleanup. Invalid or competing stage sequences freeze rather than being exported as completed successful traces. The native sanitizer probe also runs with recording enabled across repeated teardown. Existing GC, protocol, validation, and sanitizer gates remain prerequisites to runner measurements.


## Experiment limits, not public API promises

The probe has a fixed loopback listener, a small request/body cap, one named carrier, limited Ring fields, only string/byte response bodies, a simple response queue, no streaming/TLS/WebSocket/middleware API, and forced shutdown. It does not yet propagate every native disconnect into fiber cancellation. Its native H2O build currently omits wslay and optional compression integration. These are explicit boundaries of a feasibility experiment, not a partial implementation advertised as production-ready.
