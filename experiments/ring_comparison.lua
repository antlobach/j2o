local workers = {}
-- Ephemeral listening ports must not change measured HTTP request bytes.
wrk.headers["Host"] = "benchmark.local"

function setup(thread)
  table.insert(workers, thread)
end

function init(args)
  local size = tonumber(args[1])
  assert(size == 13 or size == 16384)
  expected = size == 13 and "Hello, World!\n" or string.rep("x", size)
  mismatches = 0
  validated = 0
end

function response(status, headers, body)
  validated = validated + 1
  if status ~= 200 or body ~= expected then
    mismatches = mismatches + 1
  end
end

function done(summary, latency, requests)
  local bad, checked = 0, 0
  for _, worker in ipairs(workers) do
    bad = bad + worker:get("mismatches")
    checked = checked + worker:get("validated")
  end
  local failures = summary.errors.connect + summary.errors.read +
                   summary.errors.write + summary.errors.status + summary.errors.timeout
  io.write(string.format("RING_COMPARISON requests=%d validated=%d duration_us=%d p50_us=%d p95_us=%d p99_us=%d mismatches=%d failures=%d\n",
    summary.requests, checked, summary.duration, latency:percentile(50),
    latency:percentile(95), latency:percentile(99), bad, failures))
  if bad ~= 0 or failures ~= 0 or checked ~= summary.requests or checked == 0 then
    os.exit(1)
  end
end
