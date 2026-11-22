(ns j2o.experiments.bench
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [j2o.experiments.differential :as differential]))

(defn run-command [arguments]
  (let [builder (ProcessBuilder. arguments)
        _ (.redirectErrorStream builder true)
        process (.start builder)
        output (slurp (.getInputStream process))
        exit (.waitFor process)]
    (when-not (zero? exit)
      (throw (ex-info "Benchmark command failed" {:command arguments :exit exit :output output})))
    output))

(defn process-snapshot [pid]
  (let [status (slurp (str "/proc/" pid "/status"))]
    (into {} (for [[key pattern] [[:threads #"(?m)^Threads:\s+(\d+)"]
                                 [:rss-kib #"(?m)^VmRSS:\s+(\d+)"]
                                 [:peak-rss-kib #"(?m)^VmHWM:\s+(\d+)"]]]
               [key (Long/parseLong (second (re-find pattern status)))]))))

(defn sample! [label port pid concurrency round warmup?]
  (let [arguments ["wrk" "-t1" (str "-c" concurrency) (if warmup? "-d2s" "-d3s")
                   "--latency" "-s" "experiments/validate.lua"
                   (str "http://127.0.0.1:" port "/plaintext")]
        builder (ProcessBuilder. arguments)
        _ (.redirectErrorStream builder true)
        process (.start builder)]
    (Thread/sleep 500)
    (let [snapshot (process-snapshot pid)
          output (slurp (.getInputStream process))
          exit (.waitFor process)
          fields (re-find #"J2O_RESULT requests=(\d+) duration_us=(\d+) p50_us=(\d+) p95_us=(\d+) p99_us=(\d+) mismatches=(\d+) failures=(\d+)" output)]
        (spit (str ".cache/results/" (name label) "-c" concurrency "-r" round
                   (when warmup? "-warmup") ".txt") output)
      (when-not (and (zero? exit) fields)
        (throw (ex-info "Load generator failed validation" {:exit exit :output output})))
      (let [[requests duration p50 p95 p99 mismatches failures] (mapv #(Long/parseLong %) (rest fields))
            _ (when-not (and (pos? requests) (pos? duration) (zero? mismatches) (zero? failures))
                (throw (ex-info "Load generator observed incorrect responses or transport failures"
                                {:requests requests :duration-us duration :mismatches mismatches :failures failures
                                 :output output})))
            result (merge snapshot {:server label :concurrency concurrency :round round
                                     :warmup warmup? :requests requests :duration-us duration
                                     :requests-per-second (/ (* 1000000.0 requests) duration)
                                     :p50-us p50 :p95-us p95 :p99-us p99
                                     :mismatches mismatches :failures failures})]
        result))))

(defn -main [& _]
  (when-not (and (= "true" (System/getenv "GITHUB_ACTIONS"))
                 (= "Linux" (System/getenv "RUNNER_OS")))
    (throw (ex-info "Performance measurements run on Linux GitHub runners only" {})))
  (.mkdirs (io/file ".cache/results"))
  (spit ".cache/results/environment.edn"
        (pr-str {:commit (System/getenv "GITHUB_SHA")
                 :runner (System/getenv "RUNNER_NAME")
                 :jolt (run-command ["jolt" "--version"])
                 :h2o (run-command [(str/trim (slurp ".cache/h2o-reference-path")) "--version"])
                 :kernel (run-command ["uname" "-a"])
                 :cpu (run-command ["lscpu"])
                 :compiler (run-command ["cc" "--version"])
                 :load-generator (run-command ["dpkg-query" "-W" "-f=${Version}\n" "wrk"])}))
  (let [reference (differential/start-reference!)
        builder (ProcessBuilder. ["jolt" "-m" "j2o.experiments.bench-server"])
        _ (.redirectError builder (io/file ".cache/results/j2o-server-stderr.txt"))
        process (.start builder)
        reader (java.io.BufferedReader. (java.io.InputStreamReader. (.getInputStream process)))
        ready (.readLine reader)]
    (try
      (when-not (and ready (str/starts-with? ready "READY "))
        (throw (ex-info "Benchmark server did not start" {:output ready})))
      (let [port (Long/parseLong (second (str/split ready #" ")))
            servers {:h2o {:port (:port reference) :pid (.pid (:process reference))}
                     :j2o {:port port :pid (.pid process)}}
            rows (atom [])]
        (doseq [concurrency [1 16 64] round (range 3)]
          ;; Alternate order to reduce systematic warmup and runner drift bias.
          (doseq [label (if (even? round) [:h2o :j2o] [:j2o :h2o])]
            (let [{:keys [port pid]} (get servers label)]
              (sample! label port pid concurrency round true)
              (let [result (sample! label port pid concurrency round false)]
                (swap! rows conj result)
                (spit ".cache/results/samples.edn" (pr-str @rows))
                (prn result)
                (flush)))))
        (spit ".cache/results/methodology.edn"
              (pr-str {:reference :unmodified-h2o-static-file
                       :subject :experimental-jolt-fiber-bridge
                       :body "Hello, World!\n" :h2o-io-threads 1 :j2o-io-threads 1
                       :j2o-carriers 1 :wrk-threads 1 :concurrencies [1 16 64]
                       :warmup-seconds 2 :sample-seconds 3 :repetitions 3
                       :correctness :every-response-status-and-body-validated
                       :interpretation :native-h2o-is-a-transport-ceiling-not-an-equivalent-ring-stack
                       :caution :shared-runner-noise-no-production-performance-guarantee})))
      (finally
        (let [writer (java.io.OutputStreamWriter. (.getOutputStream process))]
          (.write writer "stop\n") (.flush writer))
        (.waitFor process)
        (spit ".cache/results/j2o-server-shutdown.txt" (or (.readLine reader) ""))
        (differential/stop-reference! reference)
        (shutdown-agents)))))
