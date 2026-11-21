(ns j2o.experiments.server
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [j2o.experiments.bridge :as bridge]
            [jolt.fibers :as fib]
            [jolt.scheme :as scheme]))

(defonce gate (async/chan 128))
(defonce parked (atom 0))
(defonce cleaned (atom 0))
(def collect! (scheme/eval-string "(lambda () (let ([n (collections)]) (collect-rendezvous) (- (collections) n)))"))

(defn handler-v1 [request]
  (when-not (fib/in-fiber?) (throw (ex-info "Handler is not on a fiber" {})))
  (case (:uri request)
    "/plaintext" {:status 200 :body "Hello, World!\n"}
    "/echo" {:status 200 :body (if-let [body (:body request)] (.readAllBytes body) (byte-array 0))}
    "/query" {:status 200 :body (or (:query-string request) "")}
    "/version" {:status 200 :body (:protocol request)}
    "/park" (try
               (swap! parked inc)
               (async/<!! gate)
               {:status 200 :body "released"}
               (finally (swap! parked dec)))
    "/parked" {:status 200 :body (str @parked)}
    "/throw" (throw (ex-info "deliberate handler failure" {}))
    "/nil" nil
    "/invalid" {:status 200 :body 42}
    "/gc" {:status 200 :body (str (collect!))}
    "/hang" (try
               (async/<!! (async/chan))
               {:status 200 :body "unexpected completion"}
               (catch Throwable _ {:status 200 :body "swallowed"})
               (finally (swap! cleaned inc)))
    "/cleaned" {:status 200 :body (str @cleaned)}
    {:status 404 :body "not found"}))

(defn handler [request] (handler-v1 request))
(defn handler-v2 [request]
  (if (= "/plaintext" (:uri request))
    {:status 200 :body "reloaded"}
    (handler-v1 request)))

(defn -main [& _]
  (let [server (bridge/start! #'handler)
        deadlines (bridge/start! #'handler {:timeout-ms 100})]
    (println "READY" (:port server) (:port deadlines))
    (flush)
    (try
      (loop []
        (when-let [line (read-line)]
          (let [[command argument] (str/split line #" " 2)]
            (case command
              "reload" (do (alter-var-root #'handler (constantly handler-v2))
                           (println "ACK reload"))
              "swap" (do (reset! (:registered server)
                                  (fn [_] {:status 200 :body "swapped"}))
                         (println "ACK swap"))
              "restore" (do (alter-var-root #'handler (constantly handler-v1))
                            (reset! (:registered server) #'handler)
                            (println "ACK restore"))
              "release" (do (dotimes [_ (Long/parseLong argument)]
                              (when-not (async/offer! gate :released)
                                (throw (ex-info "Release queue full" {}))))
                            (println "ACK release"))
              "stats" (println "STATS" (pr-str (bridge/stats server)))
              "gc" (println "GC" (collect!))
              "stop" nil
              (throw (ex-info "Unknown experiment command" {:command command})))
            (flush)
            (when-not (= command "stop") (recur)))))
      (finally
        (println "STOPPED" (pr-str (bridge/stop! deadlines)) (pr-str (bridge/stop! server)))
        (flush)
        (shutdown-agents)))))
