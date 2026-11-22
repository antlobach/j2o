#!/usr/bin/env python3
"""Direct stock-H2O / stock-ring-chez comparison; never loads the j2o bridge."""
import argparse
import collections
import contextlib
import http.client
import itertools
import json
import os
from pathlib import Path
import re
import resource
import signal
import socket
import statistics
import sys
import subprocess
import time

H2O_REVISION = "d3e43fcf00c3e6b17826b95d2d362d646185918a"
RING_REVISION = "e37d52f92c1c50d0a9af360e1d994554ec48f202"
VARIANTS = ("h2o-one", "h2o-cores", "ring-default", "ring-threads-64",
            "ring-fibers", "ring-fibers-direct")
ROOT = Path(__file__).resolve().parent.parent
PATTERN = re.compile(r"RING_COMPARISON requests=(\d+) validated=(\d+) duration_us=(\d+) "
                     r"p50_us=(\d+) p95_us=(\d+) p99_us=(\d+) mismatches=(\d+) failures=(\d+)")


def command(*args, cwd=None):
    return subprocess.check_output(args, cwd=cwd, text=True, timeout=30).strip()


def snapshot(pid):
    text = Path(f"/proc/{pid}/stat").read_text()
    fields = text[text.rfind(")") + 2:].split()
    status = Path(f"/proc/{pid}/status").read_text()
    return {"cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
            **{key: int(re.search(rf"(?m)^{field}:\s+(\d+)", status)[1])
               for key, field in (("rss_kib", "VmRSS"), ("peak_rss_kib", "VmHWM"),
                                  ("threads", "Threads"))}}


def check_server(process, log):
    if process.poll() is not None:
        raise RuntimeError(f"Server exited {process.returncode}: {log.read_text()}")


@contextlib.contextmanager
def running_server(variant, body_path, output, ring_source, server_cpus):
    log = output / f"{variant}-server.log"
    port = None
    env = os.environ.copy()
    affinity = ["taskset", "-c", ",".join(map(str, server_cpus))]
    if variant.startswith("h2o-"):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        config = output / "h2o.yml"
        threads = 1 if variant == "h2o-one" else len(server_cpus)
        config.write_text(f"listen:\n  host: 127.0.0.1\n  port: {port}\n"
                          f"num-threads: {threads}\nfile.mime.setdefault: text/plain\n"
                          f"hosts:\n  default:\n    paths:\n      /:\n"
                          f"        file.dir: {json.dumps(str(body_path.parent))}\n")
        args = affinity + [Path(ROOT / ".cache/h2o-reference-path").read_text().strip(),
                           "-c", str(config)]
        cwd = ROOT
    else:
        env.update(RING_BENCH_VARIANT=variant, RING_BENCH_BODY=str(body_path))
        source = ROOT / "experiments/ring_comparison_server.clj"
        args = affinity + ["jolt", "-e", f"(load-file {json.dumps(str(source))})"]
        cwd = ring_source
    (output / "server-command.json").write_text(json.dumps(
        {"arguments": args, "cwd": str(cwd), "server_cpus": server_cpus}, indent=2) + "\n")
    with log.open("w") as stream:
        process = subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.PIPE,
                                   stdout=stream, stderr=subprocess.STDOUT, text=True)
        primary = None
        try:
            deadline = time.monotonic() + 120
            expected = body_path.read_bytes()
            while True:
                check_server(process, log)
                if port is None:
                    ready = re.search(r"(?m)^READY (\d+)\s*$", log.read_text())
                    if ready:
                        port = int(ready[1])
                if port is not None:
                    client = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                    try:
                        client.request("GET", "/plaintext")
                        response = client.getresponse()
                        if response.status != 200 or response.read() != expected:
                            raise RuntimeError(f"Readiness response mismatch: {variant}")
                        break
                    except (ConnectionError, TimeoutError, http.client.HTTPException):
                        pass
                    finally:
                        client.close()
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Server startup: {variant}: {log.read_text()}")
                time.sleep(0.05)
            yield process, port, log
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                if process.poll() is None:
                    if variant.startswith("h2o-"):
                        process.send_signal(signal.SIGTERM)
                    else:
                        process.stdin.write("stop\n")
                        process.stdin.flush()
                    process.wait(timeout=15)
                if process.returncode != 0:
                    raise RuntimeError(f"Server shutdown {variant}: {process.returncode}: {log.read_text()}")
                if variant.startswith("ring-") and "STOPPED" not in log.read_text():
                    raise RuntimeError(f"Missing completed adapter shutdown: {variant}")
            except BaseException as cleanup:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                if primary is None:
                    raise
                print(f"Cleanup failure while preserving {primary!r}: {cleanup!r}", file=sys.stderr, flush=True)
            finally:
                process.stdin.close()


def sample(process, port, variant, concurrency, round_number, size, seconds,
           output, client_cpus, warmup):
    threads = min(2, len(client_cpus), concurrency)
    path = output / ("warmup.txt" if warmup else "wrk.txt")
    args = ["taskset", "-c", ",".join(map(str, client_cpus)), "wrk", f"-t{threads}",
            f"-c{concurrency}", f"-d{seconds}s", "--timeout", "2s", "--latency",
            "-s", str(ROOT / "experiments/ring_comparison.lua"),
            f"http://127.0.0.1:{port}/plaintext", "--", str(size)]
    before = snapshot(process.pid)
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    child_before = usage.ru_utime + usage.ru_stime
    started = time.monotonic()
    peak = before.copy()
    with path.open("w") as stream:
        load = subprocess.Popen(args, stdout=stream, stderr=subprocess.STDOUT)
        try:
            while load.poll() is None:
                check_server(process, output / f"{variant}-server.log")
                current = snapshot(process.pid)
                for key in ("rss_kib", "peak_rss_kib", "threads"):
                    peak[key] = max(peak[key], current[key])
                if time.monotonic() - started > seconds + 15:
                    raise TimeoutError(f"Load timeout: {variant} c{concurrency}")
                time.sleep(0.05)
        finally:
            if load.poll() is None:
                load.kill()
                load.wait(timeout=5)
    elapsed = time.monotonic() - started
    after = snapshot(process.pid)
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    client_seconds = usage.ru_utime + usage.ru_stime - child_before
    text = path.read_text()
    fields = PATTERN.search(text)
    if load.returncode != 0 or fields is None:
        raise RuntimeError(f"Invalid load result ({load.returncode}): {text}")
    requests, validated, duration, p50, p95, p99, mismatches, failures = map(int, fields.groups())
    if not (requests == validated > 0 and duration > 0 and mismatches == failures == 0):
        raise RuntimeError(f"Response validation failed: {text}")
    return {"variant": variant, "body_bytes": size, "concurrency": concurrency,
            "round": round_number, "warmup": warmup, "requests": requests,
            "validated": validated, "duration_us": duration,
            "requests_per_second": requests * 1_000_000 / duration,
            "p50_us": p50, "p95_us": p95, "p99_us": p99,
            "mismatches": mismatches, "failures": failures,
            "server_cpu_cores": (after["cpu_seconds"] - before["cpu_seconds"]) / elapsed,
            "client_cpu_cores": client_seconds / elapsed,
            "client_capacity_fraction": client_seconds / elapsed / threads,
            "wrk_threads": threads, "observer_window_seconds": elapsed,
            "command": args, "started_unix_seconds": time.time() - elapsed,
            **{key: max(peak[key], after[key]) for key in ("rss_kib", "peak_rss_kib", "threads")}}


def quantile(values, fraction):
    position = (len(values) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(values) - 1)
    return values[low] + (values[high] - values[low]) * (position - low)


def paired_interval(ratios):
    estimates = sorted(statistics.median(sample) for sample in itertools.product(ratios, repeat=len(ratios)))
    return [quantile(estimates, .025), quantile(estimates, .975)]


def report(rows, output, rounds, concurrencies):
    groups = collections.defaultdict(list)
    for row in rows:
        groups[row["variant"], row["concurrency"]].append(row)
    expected = {(variant, c) for variant in VARIANTS for c in concurrencies}
    if set(groups) != expected:
        raise RuntimeError("Missing or unexpected benchmark cells")
    for key, samples in groups.items():
        if len(samples) != rounds or sorted(s["round"] for s in samples) != list(range(rounds)):
            raise RuntimeError(f"Missing/duplicate rounds: {key}")
    summaries = []
    lines = ["# Native H2O versus unmodified ring-chez", "",
             "HTTP/1.1 keep-alive; constant prebuilt response, no middleware/TLS/app work.",
             "Native H2O serves a warmed static file: a transport ceiling, not an equivalent Ring stack.",
             "Both servers get the same CPU affinity; wrk uses disjoint CPUs. No j2o bridge is loaded.",
             "Medians over fresh processes; CPU covers the observer-inclusive wrk process window.",
             "RSS high-water includes startup/warmup. Every completed response is status/body validated.",
             "Intervals are exhaustive paired bootstrap intervals of median round ratios, not production guarantees.", ""]
    for concurrency in concurrencies:
        lines += [f"## Concurrency {concurrency}", "",
                  "| Server | req/s | p50 µs | p99 µs | Server CPU cores | Peak RSS MiB | Peak threads | Client busy |",
                  "|---|---:|---:|---:|---:|---:|---:|---:|"]
        native = {s["round"]: s for s in groups["h2o-cores", concurrency]}
        for variant in VARIANTS:
            samples = groups[variant, concurrency]
            summary = {"variant": variant, "concurrency": concurrency,
                       **{key: statistics.median(s[key] for s in samples) for key in
                          ("requests_per_second", "p50_us", "p95_us", "p99_us", "server_cpu_cores",
                           "rss_kib", "peak_rss_kib", "threads", "client_capacity_fraction")}}
            if variant.startswith("ring-"):
                ratios = [s["requests_per_second"] / native[s["round"]]["requests_per_second"]
                          for s in sorted(samples, key=lambda s: s["round"])]
                summary.update(round_ratios_to_h2o_cores=ratios,
                               paired_ratio_interval=paired_interval(ratios))
            summaries.append(summary)
            lines.append(f"| {variant} | {summary['requests_per_second']:,.0f} | {summary['p50_us']:.0f} | "
                         f"{summary['p99_us']:.0f} | {summary['server_cpu_cores']:.2f} | "
                         f"{summary['peak_rss_kib']/1024:.1f} | {summary['threads']:.0f} | "
                         f"{summary['client_capacity_fraction']:.0%} |")
        lines.append("")
    saturated = [f"{s['variant']}/c{s['concurrency']}/r{s['round']}" for s in rows
                 if s["client_capacity_fraction"] >= .85]
    lines += ["## Limits", "",
              "Default threaded ring-chez has connection-owned workers; the 64-worker and fiber variants avoid hiding its tuning options.",
              "The direct-fiber variant is appropriate for this nonblocking handler, not arbitrary blocking application code.",
              "This does not measure HTTP/2, TLS, real middleware or the achievable performance of a complete H2O-backed Ring binding.",
              "GitHub hosted-runner noise and a loopback client remain limitations.",
              ("Possible load-generator CPU saturation (≥85% of wrk thread capacity): " + ", ".join(saturated))
              if saturated else "No window reached the 85% client CPU warning threshold.", ""]
    (output / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
    (output / "report.md").write_text("\n".join(lines))
    print("\n".join(lines), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--body-bytes", type=int, choices=(13, 16384), required=True)
    parser.add_argument("--smoke", action="store_true")
    options = parser.parse_args()
    if (os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_OS") != "Linux"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        raise SystemExit("Benchmarks run on Linux GitHub-hosted runners only")
    ring_source = Path(os.environ["RING_REFERENCE_DIR"]).resolve()
    if command("git", "rev-parse", "HEAD", cwd=ring_source) != RING_REVISION:
        raise RuntimeError("Wrong reference adapter revision")
    subprocess.run(["git", "diff", "--exit-code", "HEAD"], cwd=ring_source, check=True)
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < 2:
        raise RuntimeError("Need disjoint load-generator/server CPUs")
    split = max(1, len(cpus) // 2)
    client_cpus, server_cpus = cpus[:split], cpus[split:]
    output = ROOT / ".cache" / ("ring-comparison-smoke" if options.smoke else f"ring-comparison-{options.body_bytes}")
    output.mkdir(parents=True, exist_ok=False)
    body_path = output / "www/plaintext"
    body_path.parent.mkdir()
    body_path.write_bytes(b"Hello, World!\n" if options.body_bytes == 13 else b"x" * options.body_bytes)
    rounds, concurrencies = (1, (1,)) if options.smoke else (5, (1, 16, 64))
    environment = {"commit": os.environ["GITHUB_SHA"], "runner": os.environ["RUNNER_NAME"],
                   "h2o_revision": H2O_REVISION, "ring_revision": RING_REVISION,
                   "jolt": command("jolt", "--version"), "cpu": command("lscpu"),
                   "kernel": command("uname", "-a"), "wrk_package": command("dpkg-query", "-W", "wrk"),
                   "h2o_version": command(Path(ROOT / ".cache/h2o-reference-path").read_text().strip(), "--version"),
                   "compiler": command("cc", "--version"),
                   "client_cpus": client_cpus, "server_cpus": server_cpus,
                   "body_bytes": options.body_bytes, "rounds": rounds,
                   "concurrencies": concurrencies, "warmup_seconds": 1 if options.smoke else 2,
                   "measurement_seconds": 1 if options.smoke else 5,
                   "order": "round-rotated, alternating forward/reverse",
                   "handler": "constant prebuilt map/byte-array; no per-request handler allocation",
                   "protocol": "HTTP/1.1 keep-alive, no pipelining",
                   "gc": "enabled, default Jolt configuration; no observer/profiler in server"}
    (output / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    rows = []
    for concurrency in concurrencies:
        for round_number in range(rounds):
            order = list(VARIANTS[round_number % len(VARIANTS):] + VARIANTS[:round_number % len(VARIANTS)])
            if round_number % 2:
                order.reverse()
            for variant in order:
                window = output / f"c{concurrency}-r{round_number}-{variant}"
                window.mkdir()
                with running_server(variant, body_path, window, ring_source, server_cpus) as (process, port, _):
                    for warmup in (True, False):
                        row = sample(process, port, variant, concurrency, round_number, options.body_bytes,
                                     environment["warmup_seconds" if warmup else "measurement_seconds"],
                                     window, client_cpus, warmup)
                        (window / ("warmup.json" if warmup else "sample.json")).write_text(json.dumps(row, indent=2) + "\n")
                        if not warmup:
                            rows.append(row)
                            (output / "samples.json").write_text(json.dumps(rows, indent=2) + "\n")
                            print(json.dumps(row), flush=True)
    report(rows, output, rounds, concurrencies)
    subprocess.run(["git", "diff", "--exit-code", "HEAD"], cwd=ring_source, check=True)


if __name__ == "__main__":
    main()
