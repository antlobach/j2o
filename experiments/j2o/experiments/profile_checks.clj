(ns j2o.experiments.profile-checks
  (:require [j2o.experiments.bridge :as bridge]
            [j2o.experiments.differential :as checks]
            [j2o.experiments.profiling :as profiling]
            [j2o.experiments.profile-server :as server]
            [j2o.experiments.profile-run :as runner]
            [j2o.experiments.wire :as wire]
            [j2o.experiments.tuning-report :as report]
            [jolt.ffi :as ffi]
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

(defn timed-stop-accounting! []
  (let [load {:requests 10 :concurrency 1}
        trace {:considered 11 :completed 10 :incomplete 1 :dropped 0
               :last-id 42 :stages (vec (range 14))
               :incomplete-records [{:id 42 :stage-mask 8191}]}
        result {:admitted 11 :answered 10 :stale 1 :trace-info trace}]
    (checks/check! :timed-stop-keeps-count-domains-separate
                   {:validated-client-completions 10 :client-stop-inflight-admissions 1
                    :client-stop-inflight-answers 0 :client-stop-inflight-stale 1}
                   (runner/validate-capture! load result))
    (checks/check! :timed-stop-can-answer-unreported-tail 1
                   (:client-stop-inflight-answers
                     (runner/validate-capture! load (assoc result :answered 11 :stale 0 :trace-info nil))))
    (doseq [[label invalid]
            [[:cannot-hide-lost-reply (assoc result :answered 9)]
             [:cannot-exceed-inflight-bound (assoc result :admitted 12 :answered 11)]
             [:cannot-hide-earlier-disconnect (assoc result :answered 9 :stale 2)]
             [:cannot-hide-earlier-incomplete
              (assoc-in result [:trace-info :incomplete-records 0 :id] 41)]
             [:cannot-hide-interrupted-handler
              (assoc-in result [:trace-info :incomplete-records 0 :stage-mask] 127)]
             [:cannot-hide-recording-overflow (assoc-in result [:trace-info :dropped] 1)]
             [:cannot-hide-missing-admission (assoc-in result [:trace-info :considered] 10)]]]
      (checks/check! label true
                     (try (runner/validate-capture! load invalid) false
                          (catch clojure.lang.ExceptionInfo _ true))))))

(defn startup-rollback! []
  (doseq [stage [:ingress :pool :profile :receiver :dispatch]]
    (let [start bridge/native-start destroy bridge/native-destroy stop bridge/native-stop
          native (atom nil) stopped (atom nil) destroyed (atom false)
          failure (ex-info "injected startup failure" {:stage stage})
          fail (fn [& _] (throw failure))
          run #(bridge/start! #'checks/live-handler
                              {:ingress :waiter :stage-profile {:mode :timings}})]
      (with-redefs [bridge/native-start (fn [] (let [p (start)] (reset! native p) p))
                    bridge/native-stop (fn [p] (stop p) (reset! stopped (bridge/native-stat p 5)))
                    bridge/native-destroy (fn [p] (destroy p) (reset! destroyed true))]
        (let [caught (try
                       (case stage
                         :ingress (with-redefs [bridge/native-ingress-mode fail] (run))
                         :pool (with-redefs [fib/pool fail] (run))
                         :profile (with-redefs [profiling/create! fail] (run))
                         :receiver (with-redefs [bridge/start-ingress! fail] (run))
                         :dispatch (with-redefs [fib/spawn fail] (run)))
                       nil
                       (catch Throwable t t))]
          (checks/check! :startup-preserves-original-failure failure caught)
          (checks/check! :startup-native-was-real false (ffi/null? @native))
          (checks/check! :startup-releases-all-native-packets 0 @stopped)
          (checks/check! :startup-destroys-after-worker-retirement true @destroyed))))))

(defn preparation-failure! []
  (let [prepare bridge/native-response-create
        attempts (atom 0)
        server (bridge/start! (fn [_] {:status 200 :body "success"}))]
    (try
      (with-redefs [bridge/native-response-create
                    (fn [& args]
                      (if (= 1 (swap! attempts inc)) 0 (apply prepare args)))]
        (checks/check! :failed-preparation-does-not-claim-terminal 500
                       (:status (wire/request! (:port server) "/"))))
      (checks/check! :next-request-after-preparation-failure 200
                     (:status (wire/request! (:port server) "/")))
      (finally
        (checks/check! :preparation-failure-leaves-no-native-packets 0
                       (:live-packets (bridge/stop! server)))))))

(defn failed-dispatch-cleanup! []
  (let [destroy bridge/native-destroy
        destroyed (atom false)
        failure (ex-info "injected dispatcher failure" {})]
    (with-redefs [bridge/take-request (fn [& _] (throw failure))
                  bridge/native-destroy (fn [native] (destroy native) (reset! destroyed true))]
      (let [server (bridge/start! #'checks/live-handler)]
        (checks/check! :dispatcher-failure-is-observed failure
                       (try (fib/join (:dispatch server) 3000 ::timeout)
                            (catch Throwable t t)))
        (checks/check! :stop-preserves-dispatcher-failure failure
                       (try (bridge/stop! server) nil (catch Throwable t t)))
        (checks/check! :failed-dispatch-still-releases-native-state true @destroyed)))))

(defn failed-ingress-cleanup! []
  (let [failure (ex-info "injected native ingress failure" {})]
    (with-redefs [bridge/native-await (fn [_] (throw failure))]
      (let [server (bridge/start! #'checks/live-handler {:ingress :waiter})]
        (try (fib/join (:dispatch server) 3000 ::timeout) (catch Throwable _ nil))
        (let [caught (try (bridge/stop! server) nil (catch Throwable t t))]
          (checks/check! :ingress-root-failure-survives-dispatch-error failure (ex-cause caught))
          (checks/check! :derived-dispatch-error-is-retained 1 (count (:cleanup-errors (ex-data caught)))))))))

(defn report-integrity! []
  (let [base {:requests 10 :requests-per-second 10 :p50-us 1 :p95-us 2 :p99-us 3
              :mismatches 0 :failures 0 :admitted 10 :answered 10 :stale 0
              :stats {:live-packets 0 :connections 0}}
        rows (vec (concat
                    (for [variant (conj report/variants :native) c [1 16 64] r (range 5)]
                      (assoc base :phase :http :variant variant :mode :off :concurrency c :round r))
                    (for [variant report/variants mode [:off :timings :allocations :cpu :idle] r (range 3)]
                      (assoc base :phase :profile :variant variant :mode mode :concurrency 1 :round r))))]
    (report/validate-rows! rows)
    (doseq [[label invalid]
            [[:report-rejects-missing-round (pop rows)]
             [:report-rejects-duplicate-round (assoc rows 0 (nth rows 1))]
             [:report-rejects-mismatch (assoc-in rows [0 :mismatches] 1)]
             [:report-rejects-load-failure (assoc-in rows [0 :failures] 1)]
             [:report-rejects-retained-packet (assoc-in rows [15 :stats :live-packets] 1)]]]
      (checks/check! label true
                     (try (report/validate-rows! invalid) false
                          (catch clojure.lang.ExceptionInfo _ true))))
    (let [interval (report/paired-bootstrap [1 1 1 1 1])]
      (checks/check! :paired-bootstrap-exact-resamples 3125 (:resamples interval))
      (checks/check! :paired-bootstrap-null-effect [1.0 1.0] [(:lower interval) (:upper interval)]))))

(defn -main [& _]
  (try
    (timed-stop-accounting!)
    (startup-rollback!)
    (preparation-failure!)
    (failed-dispatch-cleanup!)
    (failed-ingress-cleanup!)
    (report-integrity!)
    (doseq [mode [:timings :allocations]]
      (bounded! mode)
      (concurrent-and-deadline! mode))
    (println "Profiling correctness checks passed; no performance measurements emitted.")
    (finally (shutdown-agents))))
