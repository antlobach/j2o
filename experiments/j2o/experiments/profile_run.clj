(ns j2o.experiments.profile-run
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [j2o.experiments.bench :as bench]
            [j2o.experiments.process :as child]))

(defn command! [process writer reader command expected]
  (.write writer (str command "\n")) (.flush writer)
  (let [reply (child/read-line! process reader 15000)]
    (when-not (= expected reply)
      (throw (ex-info "Profiling server protocol failed" {:command command :expected expected :reply reply})))))

(defn validate-capture! [load result]
  (let [info (:trace-info result)
        client-completed (or (:requests load) 0)
        tail (- (:admitted result) client-completed)]
    ;; wrk can abandon its final reply, but cannot explain lost admissions,
    ;; nonterminal work, or a stale reply from earlier in the window.
    (when-not (and (= (:admitted result) (+ (:answered result) (:stale result)))
                   (<= 0 tail (or (:concurrency load) 0))
                   (<= 0 (:stale result) tail))
      (throw (ex-info "Server accounting exceeds the timed-stop in-flight bound"
                      {:load load :result result :tail tail})))
    (when (and info
               (or (pos? (:dropped info)) (zero? (:completed info))
                   (not= (:considered info) (:admitted result))
                   (> (:incomplete info) (:stale result))
                   (not-every? #(and (= (:id %) (:last-id info))
                                     (= (:stage-mask %)
                                        (dec (bit-shift-left 1 (dec (count (:stages info)))))))
                               (:incomplete-records info))))
      (throw (ex-info "Profiling capture contains an unexplained incomplete trace" {:info info})))
    {:validated-client-completions client-completed :client-stop-inflight-admissions tail
     :client-stop-inflight-answers (- (:answered result) client-completed)
     :client-stop-inflight-stale (:stale result)}))

(defn finish-process! [process writer reader destination primary]
  (let [errors (atom [])
        attempt (fn [f] (try (f) (catch Throwable t (swap! errors conj t) nil)))]
    (when (.isAlive process)
      (attempt #(do (.write writer "stop\n") (.flush writer))))
    (attempt #(child/await! process 5000))
    (when-not (.isAlive process)
      (attempt #(let [exit (.exitValue process)
                      output (slurp reader)]
                  (spit (str destination "-shutdown.txt") output)
                  (when-not (zero? exit)
                    (throw (ex-info "Profiling child failed" {:exit exit :output output})))))
      (attempt #(when-not (empty? (slurp (str destination "-stderr.txt")))
                  (throw (ex-info "Profiling child wrote stderr" {:destination destination})))))
    (doseq [resource [writer reader]] (attempt #(.close resource)))
    (when (seq @errors)
      ;; Jolt has no suppressed-exception mutation; preserve the original cause.
      (throw (ex-info (if primary (ex-message primary) "Profiling child cleanup failed")
                      (assoc (or (ex-data primary) {}) :cleanup-errors @errors)
                      (or primary (first @errors)))))))

(defn run-mode! [mode round]
  (let [destination (str ".cache/results/profile-" (name mode) "-r" round)
        label (keyword (str "profile-" (name mode)))
        builder (ProcessBuilder. ["jolt" "-m" "j2o.experiments.profile-server" (name mode) destination])
        _ (.redirectError builder (io/file (str destination "-stderr.txt")))
        process (.start builder)
        reader (java.io.BufferedReader. (java.io.InputStreamReader. (.getInputStream process)))
        writer (java.io.OutputStreamWriter. (.getOutputStream process))
        primary (atom nil)]
    (try
      (let [ready (child/read-line! process reader 15000)]
        (when-not (and ready (str/starts-with? ready "READY "))
          (throw (ex-info "Profiling server did not start" {:mode mode :output ready})))
        (let [port (Long/parseLong (second (str/split ready #" ")))
              _ (bench/sample! label port (.pid process) 1 round true)
              _ (command! process writer reader "calibrate" "CALIBRATED")
              _ (command! process writer reader "begin" "BEGUN")
              _ (when (= :cpu mode) (command! process writer reader "start-cpu" "CPU-STARTED"))
              load (if (= :idle mode)
                     (do (Thread/sleep 3000) nil)
                     (bench/sample! label port (.pid process) 1 round false))]
          (when (= :cpu mode) (command! process writer reader "stop-cpu" "CPU-STOPPED"))
          (command! process writer reader "end" "ENDED")
          (let [result (edn/read-string (slurp (str destination ".edn")))
                accounting (validate-capture! load result)]
            (when (= :cpu mode)
              (let [cpu (edn/read-string (slurp (str destination "-cpu.edn")))]
                (when-not (pos? (reduce + 0 (map :samples (:counts cpu))))
                  (throw (ex-info "CPU sampler produced no samples" {})))))
            (merge {:mode mode :round round :destination destination}
                   accounting load result))))
      (catch Throwable t (reset! primary t) (throw t))
      (finally (finish-process! process writer reader destination @primary)))))

(defn -main [& _]
  (when-not (and (= "true" (System/getenv "GITHUB_ACTIONS"))
                 (= "Linux" (System/getenv "RUNNER_OS")))
    (throw (ex-info "Performance measurements run on Linux GitHub runners only" {})))
  (.mkdirs (io/file ".cache/results"))
  (spit ".cache/results/profile-methodology.edn"
        (pr-str {:commit (System/getenv "GITHUB_SHA")
                 :server-process :separate-from-load-generator
                 :modes [:off :timings :allocations :cpu :idle] :repetitions 3
                 :fresh-server-per-mode true :concurrency 1 :wrk-threads 1
                 :warmup-seconds 2 :measurement-seconds 3
                 :body "Hello, World!\n" :validation :every-status-and-body
                 :sample-selection :splitmix64-request-id :stride 8 :record-capacity 32768
                 :clock :native-clock-monotonic :cpu-sampler-interval-ms 1
                 :cpu-sampler-scope :active-jolt-threads-not-native-h2o-thread
                 :off-control :dormant-hook-branches-no-stage-collection
                 :stage-allocation :process-wide-cumulative-chez-bytes-including-observer
                 :whole-window-allocation :process-wide-including-runtime-and-control-io
                 :idle-control :same-server-no-requests
                 :queue-intervals :handoff-wakeup-and-scheduling-combined
                 :final-native-stage :send-inline-return-not-wire-flush
                 :known-native-bytes :packet-and-response-command-only
                 :counter-denominator :server-admitted-after-connection-close
                 :client-stop-tail :at-most-one-unreported-answer-per-nonpipelined-connection
                 :caution :shared-runner-noise-no-production-performance-guarantee}))
  (let [rows (atom [])]
    (try
      (doseq [round (range 3)
              mode (if (even? round) [:off :timings :allocations :cpu :idle]
                       [:idle :cpu :allocations :timings :off])]
        (let [row (run-mode! mode round)]
          (swap! rows conj row)
          (spit ".cache/results/profile-samples.edn" (pr-str @rows))
          (prn (dissoc row :observer-calibration)) (flush)))
      (finally (shutdown-agents)))))
