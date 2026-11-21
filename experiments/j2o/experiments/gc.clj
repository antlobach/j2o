(ns j2o.experiments.gc
  (:require [clojure.core.async :as async]
            [j2o.experiments.bridge :as bridge]
            [jolt.scheme :as scheme]))

(def collect! (scheme/eval-string "(lambda () (let ([n (collections)]) (collect-rendezvous) (- (collections) n)))"))

(defn -main [& [mode]]
  (let [safe? (= mode "safe")
        server (bridge/start! (fn [_] {:status 200 :body "ok"}))]
    (try
      (let [finished (async/thread
                       (if safe? (bridge/native-hold 1500) (bridge/unsafe-native-hold 1500))
                       :finished)]
        (loop [attempt 0]
          (when (zero? (bridge/native-hold-active))
            (when (= attempt 1000) (throw (ex-info "Native hold never started" {})))
            (Thread/sleep 1)
            (recur (inc attempt))))
        (let [collections (collect!)
              progressed-during-native-call? (= 1 (bridge/native-hold-active))]
          (when-not (pos? collections) (throw (ex-info "No collection observed" {})))
          (when-not (= safe? progressed-during-native-call?)
            (throw (ex-info "GC transition control did not distinguish safe/unsafe calls"
                            {:mode mode :progressed progressed-during-native-call?})))
          (when-not (= :finished (async/<!! finished))
            (throw (ex-info "Native hold did not finish" {})))
          ;; Keep multiple generations of retained data live while allocating more.
          (let [retained (vec (range 10000))]
            (dotimes [_ 1000] (vec (range 256)))
            (collect!)
            (when-not (= 9999 (peek retained))
              (throw (ex-info "Live data corrupted during native/GC overlap" {}))))
          (prn {:experiment :gc-transition :mode mode
                :collections collections
                :collected-before-native-return progressed-during-native-call?
                :status :passed})))
      (finally (bridge/stop! server) (shutdown-agents)))))
