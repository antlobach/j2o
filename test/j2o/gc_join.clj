(ns j2o.gc-join
  (:require [clojure.core.async :as async]
            [j2o.native :as native]
            [jolt.ffi :as ffi]
            [jolt.scheme :as scheme]))

;; The negative control deliberately omits the collect-safe transition on the
;; same production join symbol. Only this isolated qualification process uses it.
(ffi/defcfn unsafe-join* "j2o_server_join" [:pointer :uint64 :pointer] :int)
(ffi/defcfn waiters "j2o_test_join_waiters" [] :int)
(def collect! (scheme/eval-string "(lambda () (let ([n (collections)]) (collect-rendezvous) (- (collections) n)))"))

(defn exercise! [safe?]
  (let [config (native/config-create!)]
    (try
      (native/config-u64! config 2 0)
      (let [server (native/server-start! config)]
        (try
          (let [finished (async/thread
                           (ffi/with-alloc [error native/error-size]
                             (let [result ((if safe? native/join* unsafe-join*) server 1500 error)]
                               {:result result :error (native/decode-error error)})))]
            (loop [attempt 0]
              (when (zero? (waiters))
                (when (= attempt 1000) (throw (ex-info "Join did not enter its condition wait" {})))
                (Thread/sleep 1)
                (recur (inc attempt))))
            (let [collections (collect!)
                  progressed? (pos? (waiters))
                  result (async/<!! finished)]
              (when-not (and (pos? collections) (= safe? progressed?)
                             (= 3 (:result result)) (= "ETIMEDOUT" (get-in result [:error :errno-name])))
                (throw (ex-info "Real join GC transition failed"
                                {:safe safe? :collections collections :progressed progressed? :join result})))
              (prn {:scenario :native-join-gc :safe safe? :collections collections
                    :collected-during-wait progressed? :join :timed-out :status :passed})))
          (finally
            (native/force-stop! server)
            (native/join! server 5000)
            (native/server-destroy! server))))
      (finally (native/config-destroy! config)))))

(defn -main [& [mode]]
  (try
    (exercise! (= mode "safe"))
    (finally (shutdown-agents))))
