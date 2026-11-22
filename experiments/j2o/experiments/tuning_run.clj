(ns j2o.experiments.tuning-run
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [j2o.experiments.bench :as bench]
            [j2o.experiments.differential :as checks]
            [j2o.experiments.profile-run :as profiling]
            [j2o.experiments.tuning-micro :as micro]))

(def variants [:poller :waiter :poller-scratch :waiter-scratch])

(defn run-case! [variant mode concurrency round]
  (let [label (keyword (str "tuning-" (name variant) "-" (name mode)))
        destination (str ".cache/results/" (name label) "-c" concurrency "-r" round)
        waiter? (contains? #{:waiter :waiter-scratch} variant)
        scratch? (contains? #{:poller-scratch :waiter-scratch} variant)
        builder (ProcessBuilder. ["env" (str "J2O_PROBE_INGRESS=" (if waiter? "waiter" "poller"))
                                  (str "J2O_PROBE_SCRATCH=" (if scratch? "1" "0"))
                                  "jolt" "-m" "j2o.experiments.profile-server" (name mode) destination])
        _ (.redirectError builder (io/file (str destination "-stderr.txt")))
        process (.start builder)
        reader (java.io.BufferedReader. (java.io.InputStreamReader. (.getInputStream process)))
        writer (java.io.OutputStreamWriter. (.getOutputStream process))]
    (try
      (let [ready (.readLine reader)]
        (when-not (and ready (str/starts-with? ready "READY "))
          (throw (ex-info "Tuning server did not start" {:variant variant :ready ready})))
        (let [port (Long/parseLong (second (str/split ready #" ")))]
          (bench/sample! label port (.pid process) concurrency round true)
          (profiling/command! writer reader "calibrate" "CALIBRATED")
          (profiling/command! writer reader "begin" "BEGUN")
          (let [load (if (= :idle mode) (do (Thread/sleep 3000) nil)
                       (bench/sample! label port (.pid process) concurrency round false))]
            (profiling/command! writer reader "end" "ENDED")
            (let [result (edn/read-string (slurp (str destination ".edn")))
                  accounting (profiling/validate-capture! load result)]
              (when-not (and (zero? (get-in result [:stats :live-packets]))
                             (zero? (get-in result [:stats :connections])))
                (throw (ex-info "Tuning window retained native work" {:result result})))
              (when (= :cpu mode)
                (let [cpu (edn/read-string (slurp (str destination "-cpu.edn")))]
                  (when-not (pos? (reduce + 0 (map :samples (:counts cpu))))
                    (throw (ex-info "Tuning CPU sampler captured no work" {})))))
              (merge load result accounting {:variant variant :mode mode :round round
                                             :concurrency concurrency :destination destination})))))
      (finally
        (when (.isAlive process) (.write writer "stop\n") (.flush writer))
        (let [exit (.waitFor process) output (slurp reader)]
          (spit (str destination "-shutdown.txt") output)
          (when-not (zero? exit)
            (throw (ex-info "Tuning server failed" {:variant variant :exit exit :output output})))
          (when-not (empty? (slurp (str destination "-stderr.txt")))
            (throw (ex-info "Tuning server wrote stderr" {:variant variant :destination destination}))))))))

(defn native-case! [concurrency round]
  (let [reference (checks/start-reference!)]
    (try
      (bench/sample! :tuning-native (:port reference) (.pid (:process reference)) concurrency round true)
      (assoc (bench/sample! :tuning-native (:port reference) (.pid (:process reference)) concurrency round false)
             :variant :native :mode :off :interpretation :transport-ceiling-not-ring-equivalent)
      (finally (checks/stop-reference! reference)))))

(defn -main [& _]
  (when-not (and (= "true" (System/getenv "GITHUB_ACTIONS"))
                 (= "Linux" (System/getenv "RUNNER_OS")))
    (throw (ex-info "Performance measurements run on Linux GitHub runners only" {})))
  (.mkdirs (io/file ".cache/results"))
  (spit ".cache/results/tuning-methodology.edn"
        (pr-str {:commit (System/getenv "GITHUB_SHA") :http-rounds 5 :profile-rounds 3
                 :variants variants :http-concurrencies [1 16 64]
                 :warmup-seconds 2 :measurement-seconds 3 :wrk-threads 1
                 :order :alternating-forward-reverse :fresh-process-per-window true
                 :gc :enabled :handler :same-per-request-fiber-and-var-semantics
                 :waiter :one-extra-native-thread-and-bounded-channel-per-server
                 :scratch :dispatcher-owned-native-length-buffer
                 :native :unmodified-external-h2o-static-file-transport-ceiling
                 :validation :every-status-and-body :count-domains :client-completions-and-server-admissions
                 :cpu :process-total-and-sampled-managed-stacks-distinct
                 :caution :shared-runner-noise-and-component-probes-not-production-performance}))
  (let [rows (atom [])
        retain! (fn [row]
                  (swap! rows conj row)
                  (spit ".cache/results/tuning-samples.edn" (pr-str @rows))
                  (prn (dissoc row :observer-calibration :trace-info)) (flush))]
    (try
      (doseq [concurrency [1 16 64] round (range 5)
              variant (let [order (into [:native] variants)] (if (even? round) order (reverse order)))]
        (retain! (if (= :native variant) (native-case! concurrency round)
                   (run-case! variant :off concurrency round))))
      (doseq [round (range 3)
              variant (if (even? round) variants (reverse variants))
              mode (if (even? round) [:timings :allocations :cpu :idle] [:idle :cpu :allocations :timings])]
        (retain! (run-case! variant mode 1 round)))
      (micro/-main)
      (finally (shutdown-agents)))))
