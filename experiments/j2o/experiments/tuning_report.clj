(ns j2o.experiments.tuning-report
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; Artifact analysis only: this namespace does not run or time any workload.
(defn data [path] (edn/read-string (slurp path)))

(defn distribution [values]
  (let [xs (vec (sort values)) n (count xs)]
    (when (zero? n) (throw (ex-info "Missing tuning observations" {})))
    (let [quantile #(nth xs (max 0 (dec (long (Math/ceil (* n %))))))]
      {:n n :mean (/ (double (reduce + xs)) n)
       :p50 (quantile 0.5) :p95 (quantile 0.95) :p99 (quantile 0.99)
       :min (first xs) :max (peek xs)})))

(defn artifact [root row suffix]
  (str (io/file root (str (last (str/split (:destination row) #"/")) suffix))))

(def variants [:poller :waiter :poller-scratch :waiter-scratch])

(defn row-key [row]
  ((juxt :phase :variant :mode :concurrency :round) row))

(defn validate-rows! [rows]
  (let [expected (set (concat
                       (for [variant (conj variants :native) concurrency [1 16 64] round (range 5)]
                         [:http variant :off concurrency round])
                       (for [variant variants mode [:off :timings :allocations :cpu :idle] round (range 3)]
                         [:profile variant mode 1 round])))
        actual (mapv row-key rows)]
    (when-not (and (= (count expected) (count actual)) (= expected (set actual)))
      (throw (ex-info "Incomplete, duplicated, or unexpected tuning windows"
                      {:expected (count expected) :actual (count actual)
                       :duplicates (into {} (filter #(> (val %) 1) (frequencies actual)))})))
    (doseq [row rows]
      (when-not (= :idle (:mode row))
        (when-not (and (number? (:mismatches row)) (zero? (:mismatches row))
                       (number? (:failures row)) (zero? (:failures row))
                       (every? #(and (number? (get row %)) (pos? (get row %)))
                               [:requests :requests-per-second :p50-us :p95-us :p99-us]))
          (throw (ex-info "Invalid or failed tuning load" {:row row}))))
      (when-not (= :native (:variant row))
        (when-not (and (zero? (get-in row [:stats :live-packets] -1))
                       (zero? (get-in row [:stats :connections] -1))
                       (= (:admitted row) (+ (:answered row) (:stale row))))
          (throw (ex-info "Tuning window retained or lost work" {:row row})))))
    rows))

(defn require-artifact! [path]
  (when-not (and (.isFile (io/file path)) (pos? (.length (io/file path))))
    (throw (ex-info "Missing tuning artifact" {:path path}))))

(defn validate-artifacts! [root rows]
  (doseq [row rows]
    (when-not (= :idle (:mode row))
      (doseq [suffix [".txt" "-warmup.txt"]]
        (require-artifact! (str (io/file root (str (name (:server row)) "-c" (:concurrency row)
                                                  "-r" (:round row) suffix))))))
    (when-not (= :native (:variant row))
      (doseq [suffix [".edn" "-shutdown.txt"]]
        (require-artifact! (artifact root row suffix)))
      (case (:mode row)
        (:timings :allocations)
        (let [path (artifact root row "-traces.edn")
              _ (require-artifact! path)
              capture (data path)]
          (when-not (and (seq (:records capture)) (zero? (:dropped capture))
                         (= (:completed capture) (count (:records capture)))
                         (= (:completed capture) (get-in row [:trace-info :completed])))
            (throw (ex-info "Invalid tuning stage capture" {:path path}))))
        :cpu
        (do (require-artifact! (artifact root row "-cpu.edn"))
            (require-artifact! (artifact root row "-cpu.folded"))
            (when-not (pos? (reduce + 0 (map :samples (:counts (data (artifact root row "-cpu.edn"))))))
              (throw (ex-info "Empty tuning CPU capture" {:row row}))))
        nil))))

(defn paired-bootstrap [values]
  ;; Enumerate all 5^5 paired resamples, avoiding seed and RNG implementation drift.
  (let [xs (vec values) n (count xs)]
    (when-not (= 5 n) (throw (ex-info "Paired bootstrap requires five rounds" {:n n})))
    (let [sums (nth (iterate (fn [sums] (vec (for [sum sums x xs] (+ sum x)))) [0.0]) n)
          means (vec (sort (map #(/ % n) sums)))
          last-index (dec (count means))]
      {:statistic :mean-paired-ratio :resamples (count means)
       :estimate (/ (reduce + xs) n)
       :lower (nth means (long (Math/floor (* last-index 0.025))))
       :upper (nth means (long (Math/ceil (* last-index 0.975))))})))

(defn summarize-load [rows]
  (into (sorted-map)
        (for [[[variant concurrency] runs] (group-by (juxt :variant :concurrency)
                                                    (filter #(= :http (:phase %)) rows))]
          [[variant concurrency]
           (merge {:rounds (mapv :round runs)
                   :validated-completions (reduce + (map :requests runs))}
                  (into {} (for [k [:requests-per-second :p50-us :p95-us :p99-us :threads :rss-kib]]
                             [k (distribution (map k runs))]))
                  (when (every? :counters runs)
                    {:managed-bytes-per-admission
                     (distribution (map #(/ (double (get-in % [:counters :allocated-bytes])) (:admitted %)) runs))
                     :process-cpu-us-per-admission
                     (distribution (map #(/ (get-in % [:counters :process-cpu-nanos]) (* 1000.0 (:admitted %))) runs))}))])))

(defn paired-effects [rows]
  (let [off (filter #(= :http (:phase %)) rows)
        baseline (into {} (map (fn [r] [[(:concurrency r) (:round r)] r])
                               (filter #(= :poller (:variant %)) off)))]
    (mapv (fn [[[variant concurrency] runs]]
            (let [pairs (mapv (fn [r]
                                (let [base (get baseline [concurrency (:round r)])]
                                  (when-not base
                                    (throw (ex-info "Missing paired stock window" {:row r})))
                                  {:round (:round r)
                                   :throughput-ratio (/ (:requests-per-second r) (:requests-per-second base))
                                   :p50-ratio (/ (double (:p50-us r)) (:p50-us base))
                                   :p95-ratio (/ (double (:p95-us r)) (:p95-us base))
                                   :p99-ratio (/ (double (:p99-us r)) (:p99-us base))})) runs)]
              {:variant variant :concurrency concurrency :pairs pairs
               :win-counts {:throughput (count (filter #(> (:throughput-ratio %) 1.0) pairs))
                            :p99 (count (filter #(< (:p99-ratio %) 1.0) pairs))
                            :throughput-and-p99 (count (filter #(and (> (:throughput-ratio %) 1.0)
                                                                                    (< (:p99-ratio %) 1.0)) pairs))}
               :paired-bootstrap-95 (into {} (for [k [:throughput-ratio :p50-ratio :p95-ratio :p99-ratio]]
                                                [k (paired-bootstrap (map k pairs))]))
               :throughput-ratio (distribution (map :throughput-ratio pairs))
               :p50-ratio (distribution (map :p50-ratio pairs))
               :p95-ratio (distribution (map :p95-ratio pairs))
               :p99-ratio (distribution (map :p99-ratio pairs))
               :interpretation (if (= :native variant) :transport-ceiling-not-ring-equivalent :same-ring-semantics)}))
          (sort-by key (group-by (juxt :variant :concurrency)
                                (remove #(= :poller (:variant %)) off))))))

(def timing-intervals
  [[:native-snapshot :native-arrival :native-queued]
   [:ingress-to-native-take :native-queued :native-dequeued]
   [:native-take-to-decode :native-dequeued :decode-start]
   [:request-marshalling :decode-start :request-decoded]
   [:fiber-enqueue :request-decoded :fiber-enqueued]
   [:fiber-start :fiber-enqueued :handler-start]
   [:handler :handler-start :handler-return]
   [:response-validation-encoding :handler-return :response-encoded]
   [:output-ffi-marshalling :response-encoded :native-submit-enter]
   [:native-command-copy :native-submit-enter :response-queued]
   [:egress-handoff :response-queued :response-received]
   [:h2o-send-call :response-received :native-send-return]])

(def allocation-intervals
  [[:request-marshalling :decode-start :request-decoded]
   [:fiber-dispatch :request-decoded :handler-start]
   [:handler :handler-start :handler-return]
   [:response-validation-encoding :handler-return :response-encoded]
   [:output-buffer :response-encoded :submit-ready]])

(defn intervals [captures field groups divisor]
  (mapv (fn [[label start end]]
          {:interval label
           :distribution
           (distribution
             (mapcat (fn [capture]
                       (let [indices (zipmap (:stages capture) (range))
                             a (get indices start) b (get indices end)]
                         (when-not (and a b)
                           (throw (ex-info "Missing profile stage" {:start start :end end})))
                         (map (fn [r] (/ (double (- (nth (field r) b) (nth (field r) a))) divisor))
                              (:records capture)))) captures))}) groups))

(defn summarize-profile [root variant rows]
  (let [runs (filter #(= variant (:variant %)) rows)
        timing (mapv #(data (artifact root % "-traces.edn")) (filter #(= :timings (:mode %)) runs))
        allocations (mapv #(data (artifact root % "-traces.edn")) (filter #(= :allocations (:mode %)) runs))
        cpu (mapv #(data (artifact root % "-cpu.edn")) (filter #(= :cpu (:mode %)) runs))
        self-time (reduce (fn [acc row] (update acc (:fn row) (fnil + 0.0) (:self-ms row)))
                          {} (mapcat :counts cpu))
        total-self (reduce + 0.0 (vals self-time))]
    {:variant variant
     :timing-captures (mapv #(dissoc % :records) timing)
     :interpretation :observer-inclusive-no-calibration-subtraction
     :allocation-captures (mapv #(dissoc % :records) allocations)
     :total-server-us (first (intervals timing :ns [[:total :native-arrival :native-send-return]] 1000.0))
     :timing-us (intervals timing :ns timing-intervals 1000.0)
     :managed-allocation-bytes (intervals allocations :allocated allocation-intervals 1.0)
     :controls (mapv (fn [mode]
                      (let [windows (filter #(= mode (:mode %)) runs)]
                        (merge {:mode mode :interpretation :observer-inclusive
                                :matched-off (when-not (contains? #{:off :idle} mode)
                                               (mapv (fn [window]
                                                       (let [off (first (filter #(and (= :off (:mode %))
                                                                                      (= (:round window) (:round %))) runs))]
                                                         {:round (:round window)
                                                          :p50-ratio (/ (double (:p50-us window)) (:p50-us off))
                                                          :p99-ratio (/ (double (:p99-us window)) (:p99-us off))}))
                                                     windows))}
                               (when (seq (keep :p50-us windows))
                                 {:client-p50-us (distribution (map :p50-us windows))
                                  :client-p99-us (distribution (map :p99-us windows))})
                               {:calibration (vec (mapcat :observer-calibration windows))
                                :whole-window-counters (mapv :counters windows)})))
                    [:off :timings :allocations :cpu :idle])
     :cpu-stack-samples (reduce + 0 (map :count (mapcat :stacks cpu)))
     :cpu-sampled-ms total-self
     :cpu-self (mapv (fn [[f ms]] {:fn f :self-ms ms :self-percent (* 100.0 (/ ms total-self))})
                     (take 20 (sort-by (comp - val) self-time)))}))

(defn report [root]
  (let [rows (validate-rows! (data (str (io/file root "tuning-samples.edn"))))
        _ (validate-artifacts! root rows)
        profiles (filter #(= :profile (:phase %)) rows)
        variants (sort (distinct (map :variant profiles)))]
    {:loads (summarize-load rows)
     :paired-effects (paired-effects rows)
     :profiles (mapv #(summarize-profile root % profiles) variants)
     :accounting (into {} (for [k [:requests :admitted :answered :stale :mismatches :failures
                                   :validated-client-completions :client-stop-inflight-admissions]]
                            [k (reduce + 0 (keep k rows))]))
     :caveats [:same-run-paired-comparisons :native-is-transport-ceiling
               :handoff-includes-kernel-wakeup-and-runtime-scheduling
               :allocation-is-process-wide-not-exclusive :sampled-cpu-excludes-native-h2o
               :observer-inclusive-no-calibration-subtraction
               :microprobes-are-diagnostics-not-adapter-throughput]}))

(defn -main [& [directory]]
  (let [root (or directory ".cache/results") result (report root)]
    (spit (str (io/file root "tuning-summary.edn")) (pr-str result))
    (prn :load-summary (:loads result))
    (doseq [row (:paired-effects result)] (prn :paired-effect row))
    (doseq [row (:profiles result)]
      (prn :profile (dissoc row :controls :timing-captures :allocation-captures)))
    (prn :accounting (:accounting result))))
