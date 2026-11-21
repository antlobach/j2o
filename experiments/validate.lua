local workers = {}

function setup(thread)
  table.insert(workers, thread)
end

function init(args)
  mismatches = 0
end

function response(status, headers, body)
  if status ~= 200 or body ~= "Hello, World!\n" then
    mismatches = mismatches + 1
  end
end

function done(summary, latency, requests)
  local mismatches = 0
  for _, worker in ipairs(workers) do
    mismatches = mismatches + worker:get("mismatches")
  end
  local failures = summary.errors.connect + summary.errors.read +
                   summary.errors.write + summary.errors.status + summary.errors.timeout
  io.write(string.format("J2O_RESULT requests=%d duration_us=%d p50_us=%d p95_us=%d p99_us=%d mismatches=%d failures=%d\n",
    summary.requests, summary.duration, latency:percentile(50), latency:percentile(95),
    latency:percentile(99), mismatches, failures))
  if mismatches > 0 or failures > 0 or summary.requests == 0 then
    os.exit(1)
  end
end
