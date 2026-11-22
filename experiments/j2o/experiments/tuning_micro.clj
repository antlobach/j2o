(ns j2o.experiments.tuning-micro
  (:require [clojure.core.async :as async]
            [j2o.experiments.bridge :as bridge]
            [j2o.experiments.profiling :as profiling]
            [j2o.experiments.tuning-report :as report]
            [jolt.ffi :as ffi]
            [jolt.fibers :as fib]))

(ffi/defcfn scalar-native "j2o_probe_scalar" [:uint64] :uint64)
(ffi/defcfn scalar-safe "j2o_probe_scalar" [:uint64] :uint64 :blocking)

(defn managed-scalar [value] (inc value))

(defn advance [operation value n]
  (loop [i 0 value value]
    (if (= i n) value (recur (inc i) (operation value)))))

(defn measure! [operation samples batch-size]
  (let [warmup 1000
        initial (advance operation 0 warmup)
        before (profiling/counters)
        raw (loop [i 0 value initial samples-ns []]
              (if (= i samples)
                (do (when-not (= value (+ warmup (* samples batch-size)))
                      (throw (ex-info "Diagnostic operation lost work" {:value value})))
                    samples-ns)
                (let [start (System/nanoTime)
                      value (advance operation value batch-size)
                      elapsed (- (System/nanoTime) start)]
                  (recur (inc i) value (conj samples-ns (/ (double elapsed) batch-size))))))
        after (profiling/counters)]
    {:measured-operations (* samples batch-size) :batch-size batch-size :warmup-operations warmup
     :raw-ns-per-operation raw :ns-per-operation (report/distribution raw)
     :whole-window-counters (profiling/counter-delta before after)}))

(defn channel-probe! [pool]
  (let [requests (async/chan 1) replies (async/chan 1)
        worker (async/thread
                 (try
                   (loop []
                     (when-let [value (async/<!! requests)]
                       (when (async/>!! replies (inc value)) (recur))))
                   [:ok]
                   (catch Throwable t [:error t])
                   (finally (async/close! replies))))]
    (try
      (fib/join
        (fib/spawn
          #(measure! (fn [value]
                       (when-not (async/>!! requests value)
                         (throw (ex-info "Diagnostic channel partner closed" {})))
                       (let [reply (async/<!! replies)]
                         (when (nil? reply) (throw (ex-info "Diagnostic channel reply lost" {}))) reply))
                     2000 1)
          {:pool pool}))
      (finally
        (async/close! requests)
        (async/close! replies)
        (let [[result channel] (async/alts!! [worker (async/timeout 3000)])]
          (when-not (= worker channel) (throw (ex-info "Diagnostic native partner did not stop" {})))
          (when (= :error (first result)) (throw (second result))))))))

(defn probe! [pool kind]
  (if (= :native-thread-channel kind)
    (channel-probe! pool)
    (fib/join
      (fib/spawn
        (fn [] (case kind
           :managed-scalar (measure! managed-scalar 200 100)
           :native-scalar (measure! scalar-native 200 100)
           :collect-safe-scalar (measure! scalar-safe 200 100)
           :same-carrier-spawn-join
           (measure! (fn [value] (fib/join (fib/spawn #(inc value) {:pool pool}))) 2000 1)
           (throw (ex-info "Unknown diagnostic kind" {:kind kind}))))
        {:pool pool}))))

(defn -main [& _]
  (when-not (and (= "true" (System/getenv "GITHUB_ACTIONS"))
                 (= "Linux" (System/getenv "RUNNER_OS")))
    (throw (ex-info "Performance measurements run on Linux GitHub runners only" {})))
  (let [pool (fib/pool {:name :j2o-component-probes :size 1})
        kinds [:managed-scalar :native-scalar :collect-safe-scalar :same-carrier-spawn-join :native-thread-channel]
        rows (atom [])]
    (try
      (let [clock (mapv (fn [_] (let [start (System/nanoTime)] (- (System/nanoTime) start))) (range 1000))]
        (doseq [round (range 3) kind (if (even? round) kinds (reverse kinds))]
          (let [row (assoc (probe! pool kind) :kind kind :round round)]
            (swap! rows conj row)
            (spit ".cache/results/tuning-micro.edn"
                  (pr-str {:clock-calibration-ns clock :rows @rows :gc :enabled
                           :caveats [:observer-and-loop-included :allocation-process-wide-includes-observer
                                     :components-not-http-adapter-performance]}))
            (prn :component (dissoc row :raw-ns-per-operation)) (flush))))
      (finally (fib/shutdown! pool)))))
