(ns j2o.experiments.fibers
  (:refer-clojure :exclude [run!])
  (:require [clojure.core.async :as async]
            [jolt.ffi :as ffi]
            [jolt.fibers :as fib]))

;; A collect-safe call releases the GC, but still pins a fiber's carrier.
(ffi/defcfn native-sleep "usleep" [:uint] :int :blocking)

(def ^:dynamic *request-context* :outside)
(defn sample-handler [_] {:status 200 :body "before"})

(defn check! [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str "Failed: " label)
                    {:expected expected :actual actual})))
  (prn {:experiment label :result actual})
  actual)

(defn await! [ch]
  (let [[value port] (async/alts!! [ch (async/timeout 3000)])]
    (when-not (= ch port) (throw (ex-info "Probe handshake timed out" {})))
    value))

(defn invoke [registered request]
  (let [handler (if (var? registered) @registered registered)]
    (handler request)))

(defn run! []
  (let [pool (fib/pool {:name :j2o-experiments :size 1})
        spawn (fn [f] (fib/spawn f {:pool pool}))]
    (try
      (let [entered (async/chan 1)
            release (async/chan)
            parked (spawn #(do (async/>!! entered :entered)
                               (async/<!! release)))
            _ (await! entered)
            other (spawn #(do (Thread/sleep 10) :other))]
        (check! :channel-wait-releases-carrier :other (fib/join other 1000 :timeout))
        (async/>!! release :released)
        (check! :nested-sync-function-parks :released (fib/join parked)))

      (let [ready (async/chan 128)
            sleepers (mapv (fn [i]
                             (spawn #(do (async/>!! ready i)
                                         (Thread/sleep 150)
                                         i)))
                           (range 128))]
        (dotimes [_ 128] (await! ready))
        (check! :sleep-does-not-hold-carrier :quick
                (fib/join (spawn (constantly :quick)) 1000 :timeout))
        (check! :one-carrier-128-fibers (vec (range 128))
                (mapv #(fib/join % 3000 :timeout) sleepers)))

      (alter-var-root #'sample-handler
                      (constantly (fn [_] {:status 200 :body "before"})))
      (let [captured sample-handler
            registered #'sample-handler]
        (alter-var-root #'sample-handler
                        (constantly (fn [_] {:status 200 :body "after"})))
        (check! :var-redefinition "after" (:body (fib/join (spawn #(invoke registered {})))))
        (check! :function-value-is-captured "before" (:body (invoke captured {}))))

      (let [registered (atom (fn [_] :old))
            entered (async/chan 1)
            release (async/chan)
            old (spawn #(let [handler @registered]
                          (async/>!! entered :captured)
                          (async/<!! release)
                          (handler {})))]
        (await! entered)
        (reset! registered (fn [_] :new))
        (check! :swap-affects-next-invocation :new (fib/join (spawn #(@registered {}))))
        (async/>!! release :continue)
        (check! :swap-does-not-replace-inflight-entry :old (fib/join old)))

      (check! :dynamic-bindings :request-17
              (fib/join (binding [*request-context* :request-17]
                          (spawn #(do (Thread/sleep 10) *request-context*)))))
      (check! :parent-binding-unchanged :outside *request-context*)

      (let [entered (async/chan 1)
            blocker (spawn #(do (async/>!! entered :entered)
                                (native-sleep 400000)
                                :released))]
        (await! entered)
        (let [queued (spawn (constantly :ready))]
          (check! :collect-safe-ffi-still-pins-carrier :pinned
                  (fib/join queued 30 :pinned))
          (check! :native-call-finishes :released (fib/join blocker))
          (check! :carrier-recovers :ready (fib/join queued))))

      (let [entered (async/chan 1)
            work (spawn #(let [[kind value]
                                (async/<!!
                                  (async/thread
                                    (try
                                      (async/>!! entered :entered)
                                      (native-sleep 400000)
                                      [:ok :native-result]
                                      (catch Throwable t [:error t]))))]
                            (if (= kind :ok) value (throw value))))]
        (await! entered)
        (check! :explicit-offload-frees-carrier :quick
                (fib/join (spawn (constantly :quick)) 1000 :timeout))
        (check! :explicit-offload-result :native-result (fib/join work)))

      (check! :offload-error-propagates :upstream
              (try
                (fib/join
                  (spawn #(let [[kind value]
                                (async/<!!
                                  (async/thread
                                    (try (throw (ex-info "upstream" {:kind :upstream}))
                                         (catch Throwable t [:error t]))))]
                            (if (= kind :error) (throw value) value))))
                :missing-error
                (catch Throwable t (:kind (ex-data t)))))

      (let [entered (async/chan 1)
            release (async/chan 1)
            cleaned (atom false)
            victim (spawn #(try
                             (async/>!! entered :entered)
                             (async/<!! release)
                             (catch Throwable _ :swallowed)
                             (finally (reset! cleaned true))))]
        (await! entered)
        (fib/kill! victim (ex-info "handler deadline" {:kind :deadline}))
        (check! :kill-bypasses-catch :deadline
                (try (fib/join victim) :survived
                     (catch Throwable t (:kind (ex-data t)))))
        (check! :kill-runs-finally true @cleaned)
        (check! :cancelled-wait-does-not-consume-value true (async/offer! release :remaining))
        (check! :cancelled-channel-value :remaining (async/<!! release)))

      (let [entered (async/chan 1)
            cleaned (atom false)
            spinner (spawn #(try
                              (async/>!! entered :entered)
                              (loop [i 0] (recur (inc i)))
                              (finally (reset! cleaned true))))]
        (await! entered)
        (check! :compute-preemption :quick
                (fib/join (spawn (constantly :quick)) 1000 :timeout))
        (fib/kill! spinner (ex-info "compute deadline" {:kind :compute-deadline}))
        (check! :compute-cancellation :compute-deadline
                (try (fib/join spinner 1000 :timeout)
                     (catch Throwable t (:kind (ex-data t)))))
        (check! :compute-finally true @cleaned))
      (prn {:experiment :fibers :status :passed :carriers (fib/pool-size pool)})
      (finally (fib/shutdown! pool)))))

(defn -main [& _]
  (run!)
  (shutdown-agents))
