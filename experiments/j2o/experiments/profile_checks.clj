(ns j2o.experiments.profile-checks
  (:require [j2o.experiments.bridge :as bridge]
            [j2o.experiments.differential :as checks]
            [j2o.experiments.profiling :as profiling]
            [j2o.experiments.profile-server :as server]
            [j2o.experiments.wire :as wire]
            [jolt.fibers :as fib]))

(defn check-traces! [mode trace]
  (doseq [record (:records trace)]
    (checks/check! :single-clock-ordered-stages true (apply <= (:ns record)))
    (when (= :allocations mode)
      (checks/check! :cumulative-allocation-across-collections true
                     (apply <= (subvec (:allocated record) 3 10)))
      (checks/check! :collection-counter-is-monotonic true
                     (apply <= (subvec (:collections record) 3 10))))))

(defn bounded! [mode]
  (let [server (bridge/start! #'checks/live-handler
                              {:stage-profile {:mode mode :stride 1 :capacity 4}})
        observer (:profile server)
        port (:port server)]
    (try
      (profiling/recording! observer true)
      (checks/check! :profiled-plaintext "Hello, World!\n" (wire/text (wire/request! port "/plaintext")))
      (let [body (byte-array (map unchecked-byte [0 255 128 13 10 206 187]))]
        (checks/check! :profiled-binary (vec body) (vec (:body (wire/request! port "POST" "/echo" body)))))
      (checks/check! :profiled-explicit-gc true
                     (pos? (Long/parseLong (wire/text (wire/request! port "/gc")))))
      (checks/check! :profiled-response-after-gc 200 (:status (wire/request! port "/plaintext")))
      ;; Filled recording storage must not become an HTTP admission limit.
      (checks/hot-reload! server)
      (server/quiesce! server)
      (profiling/recording! observer false)
      (let [trace (profiling/capture observer)
            admitted (:admitted (bridge/stats server))]
        (checks/check! :profile-cap-is-bounded 4 (:retained trace))
        (checks/check! :profile-full-records-survive-gc-and-native-packet-free [1 2 3 4]
                       (mapv :id (:records trace)))
        (checks/check! :overflow-is-accounted (- admitted 4) (:dropped trace))
        (checks/check! :trace-overflow-does-not-lose-http-requests admitted (:answered (bridge/stats server)))
        (check-traces! mode trace)
        (when (= :allocations mode)
          (let [gc-record (nth (:records trace) 2)]
            (checks/check! :collection-is-observed-inside-handler true
                           (> (nth (:collections gc-record) 7) (nth (:collections gc-record) 6))))))
      (finally
        (checks/check! :profiled-shutdown-releases-snapshots 0 (:live-packets (bridge/stop! server)))))))

(defn concurrent-and-deadline! [mode]
  (let [server (bridge/start! #'checks/live-handler
                              {:timeout-ms 100 :stage-profile {:mode mode :stride 1 :capacity 32}})
        observer (:profile server)
        clients (fib/pool {:name :profile-check-clients :size 1})]
    (try
      (profiling/recording! observer true)
      (let [tasks (mapv (fn [i]
                          (fib/spawn
                            #(let [body (byte-array (repeat (inc i) (unchecked-byte (+ 128 i))))]
                               (checks/check! :concurrent-profiled-binary (vec body)
                                              (vec (:body (wire/request! (:port server) "POST" "/echo" body)))))
                            {:pool clients}))
                        (range 8))]
        (doseq [task tasks] (fib/join task)))
      (checks/check! :profiled-deadline 503 (:status (wire/request! (:port server) "/hang")))
      (checks/check! :profiled-handler-recovers-after-deadline 200
                     (:status (wire/request! (:port server) "/plaintext")))
      (server/quiesce! server)
      (profiling/recording! observer false)
      (let [trace (profiling/capture observer)]
        (checks/check! :concurrent-traces-keep-request-identity #{1 2 3 4 5 6 7 8 10}
                       (set (map :id (:records trace))))
        (checks/check! :late-handler-does-not-export-false-complete-trace 1 (:incomplete trace))
        (checks/check! :concurrent-profile-has-no-overflow 0 (:dropped trace))
        (check-traces! mode trace))
      (finally
        (bridge/stop! server)
        (fib/shutdown! clients)))))

(defn -main [& _]
  (try
    (doseq [mode [:timings :allocations]]
      (bounded! mode)
      (concurrent-and-deadline! mode))
    (println "Profiling correctness checks passed; no performance measurements emitted.")
    (finally (shutdown-agents))))
