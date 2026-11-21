(ns j2o.experiments.differential
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.core.async :as async]
            [j2o.experiments.bridge :as bridge]
            [j2o.experiments.server :as fixture]
            [j2o.experiments.wire :as wire]
            [jolt.fibers :as fib]))

(defn check! [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str "Failed: " label) {:expected expected :actual actual})))
  (prn {:check label :status :passed})
  actual)

(defn eventually! [label predicate]
  (loop [attempt 0]
    (if (predicate) true
      (do (when (= attempt 500) (throw (ex-info (str "Timed out: " label) {})))
          (Thread/sleep 10)
          (recur (inc attempt))))))

(defn free-port []
  (with-open [socket (java.net.ServerSocket. 0)] (.getLocalPort socket)))

(defn start-reference! []
  (let [root (str (System/getProperty "user.dir") "/.cache/reference-root")
        port (free-port)
        config (str (System/getProperty "user.dir") "/.cache/reference.yml")
        executable (str/trim (slurp ".cache/h2o-reference-path"))]
    (.mkdirs (io/file root))
    (spit (str root "/plaintext") "Hello, World!\n")
    (spit config (str "listen:\n  host: 127.0.0.1\n  port: " port
                      "\nnum-threads: 1\nlimit-request-body: 4096\nhosts:\n"
                      "  default:\n    paths:\n      /:\n        file.dir: " root "\n"))
    (let [builder (ProcessBuilder. [executable "-c" config])
          _ (.redirectErrorStream builder true)
          _ (.redirectOutput builder (io/file ".cache/reference.log"))
          process (.start builder)]
      (try
        (eventually! :reference-start
                     #(try (let [client (wire/connect port)] (wire/close! client) true)
                           (catch Exception _ false)))
        {:port port :process process}
        (catch Throwable t (.destroy process) (throw t))))))

(defn stop-reference! [reference]
  (.destroy (:process reference))
  (.waitFor (:process reference)))

(defn signature [response]
  [(:status response) (vec (:body response)) (get-in response [:headers "content-length"])])

(defn keepalive! [port]
  (let [client (wire/connect port)]
    (try
      (wire/send! client (str "GET /plaintext HTTP/1.1\r\nHost: localhost\r\n\r\n"
                              "GET /plaintext HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
      [(signature (wire/response! client)) (signature (wire/response! client))]
      (finally (wire/close! client)))))

(defn mixed-framing-closes! [port]
  (let [client (wire/connect port)]
    (try
      (wire/send! client
                  (str "GET /plaintext HTTP/1.1\r\nHost: localhost\r\n"
                       "Content-Length: 0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"
                       "GET /plaintext HTTP/1.1\r\nHost: localhost\r\n\r\n"))
      (let [response (wire/response! client)]
        (check! :mixed-framing-response 200 (:status response))
        (check! :mixed-framing-announces-close "close" (get-in response [:headers "connection"]))
        (check! :mixed-framing-closes-before-next-request -1 (.read (:in client))))
      (finally (wire/close! client)))))

(defn compare-protocol! [port reference-port]
  (doseq [method ["GET" "HEAD"]]
    (check! [:native-reference method]
            (signature (wire/request! reference-port method "/plaintext" nil))
            (signature (wire/request! port method "/plaintext" nil))))
  (check! :native-reference-pipelining (keepalive! reference-port) (keepalive! port))
  (mixed-framing-closes! reference-port)
  (mixed-framing-closes! port)
  (doseq [[label request]
          [[:conflicting-content-length
            "GET /plaintext HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nContent-Length: 1\r\nConnection: close\r\n\r\nx"]
           [:content-length-and-transfer-encoding
            "GET /plaintext HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n0\r\n\r\n"]
           [:invalid-header-name
            "GET /plaintext HTTP/1.1\r\nHost: localhost\r\nBad Header: value\r\nConnection: close\r\n\r\n"]
           [:bare-lf
            "GET /plaintext HTTP/1.1\nHost: localhost\nConnection: close\n\n"]]]
    (let [expected (:status (wire/raw! reference-port request))
          actual (:status (wire/raw! port request))]
      (check! [:native-parser label] expected actual)
      (prn {:parser-case label :observed-status actual}))))

(defn live-handler [request] (fixture/handler-v1 request))

(defn hot-reload! [server]
  (let [client (wire/connect (:port server))
        request "GET /plaintext HTTP/1.1\r\nHost: localhost\r\n\r\n"]
    (try
      (wire/send! client request)
      (check! :var-before "Hello, World!\n" (wire/text (wire/response! client)))
      (alter-var-root #'live-handler (constantly fixture/handler-v2))
      (wire/send! client request)
      (check! :var-redefined-on-same-connection "reloaded" (wire/text (wire/response! client)))
      (reset! (:registered server) (fn [_] {:status 200 :body "swapped"}))
      (wire/send! client request)
      (check! :swap-on-same-connection "swapped" (wire/text (wire/response! client)))
      (reset! (:registered server) #'live-handler)
      (alter-var-root #'live-handler (constantly 42))
      (wire/send! client request)
      (check! :invalid-var-root-is-request-failure 500 (:status (wire/response! client)))
      (alter-var-root #'live-handler (constantly fixture/handler-v1))
      (wire/send! client request)
      (check! :handler-recovers-without-reconnect "Hello, World!\n" (wire/text (wire/response! client)))
      (finally
        (alter-var-root #'live-handler (constantly fixture/handler-v1))
        (wire/close! client)))))

(defn exercise! [reference]
  (let [server (bridge/start! #'live-handler)
        client-pool (fib/pool {:name :probe-clients :size 1})
        spawn #(fib/spawn % {:pool client-pool})
        port (:port server)]
    (try
      (compare-protocol! port (:port reference))
      (hot-reload! server)
      (doseq [path ["/throw" "/nil" "/invalid" "/oversized-response"]]
        (check! [:handler-error path] 500 (:status (wire/request! port path))))
      (let [bytes (byte-array (map unchecked-byte [0 255 128 13 10 206 187 240 159 152 128]))]
        (check! :binary-request-response (vec bytes)
                (vec (:body (wire/request! port "POST" "/echo" bytes)))))
      (check! :query-is-separate "a=%2F&b=2" (wire/text (wire/request! port "/query?a=%2F&b=2")))
      (check! :request-body-limit 413 (:status (wire/request! port "POST" "/echo" (byte-array 4097))))
      (let [client (wire/connect port)]
        (try
          (wire/send! client "POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n")
          (doseq [part ["2\r\n" "he" "\r\n3\r\n" "llo" "\r\n0\r\n\r\n"]]
            (wire/send! client part))
          (check! :fragmented-chunked-upload "hello" (wire/text (wire/response! client)))
          (finally (wire/close! client))))
      (let [pending (spawn #(wire/request! port "/park"))]
        (eventually! :handler-parked #(= 1 @fixture/parked))
        (check! :parked-handler-does-not-hold-h2o-loop 200 (:status (wire/request! port "/plaintext")))
        (check! :handler-still-waiting :waiting (fib/join pending 10 :waiting))
        (async/>!! fixture/gate :release)
        (check! :parked-handler-resumes "released" (wire/text (fib/join pending))))
      (let [requests (mapv (fn [_] (spawn #(wire/request! port "/park"))) (range 64))]
        (eventually! :all-admitted-handlers-parked #(= 64 @fixture/parked))
        (check! :bounded-admission 503 (:status (wire/request! port "/plaintext")))
        (dotimes [_ 64] (async/>!! fixture/gate :release))
        (doseq [request requests]
          (check! :admitted-request-completes 200 (:status (fib/join request 5000 {:status :timeout})))))
      (let [traffic (mapv (fn [_] (spawn #(dotimes [_ 30]
                                          (when-not (= 200 (:status (wire/request! port "/plaintext")))
                                            (throw (ex-info "Traffic failed during GC" {}))))))
                          (range 8))]
        (dotimes [_ 20]
          (check! :collection-progress-during-http true
                  (pos? (Long/parseLong (wire/text (wire/request! port "/gc"))))))
        (doseq [task traffic] (fib/join task)))
      (let [before (:stale (bridge/stats server))]
        (bridge/respond! (:native server) 18446744073709551614N {:status 200 :body "late"})
        (eventually! :stale-reply-rejected #(= (inc before) (:stale (bridge/stats server))))
        (check! :late-reply-does-not-corrupt-next-request "Hello, World!\n"
                (wire/text (wire/request! port "/plaintext"))))
      (let [client (wire/connect port)]
        (wire/send! client "GET /park HTTP/1.1\r\nHost: localhost\r\n\r\n")
        (eventually! :disconnect-request-parked #(= 1 @fixture/parked))
        (wire/close! client)
        (async/>!! fixture/gate :release)
        (eventually! :disconnected-handler-finishes #(zero? @fixture/parked))
        (check! :disconnect-does-not-break-server 200 (:status (wire/request! port "/plaintext"))))
      (finally
        (let [result (bridge/stop! server)]
          (check! :native-request-snapshots-released 0 (:live-packets result))
          (prn {:server-final-stats result}))
        (fib/shutdown! client-pool)))))

(defn deadlines! []
  (let [server (bridge/start! #'live-handler {:timeout-ms 100})]
    (try
      (let [before @fixture/cleaned]
        (check! :deadline-response 503 (:status (wire/request! (:port server) "/hang")))
        (eventually! :deadline-finally-runs #(= (inc before) @fixture/cleaned))
        (check! :deadline-does-not-kill-carrier 200 (:status (wire/request! (:port server) "/plaintext"))))
      (finally (bridge/stop! server)))))

(defn shutdown! []
  (let [server (bridge/start! #'live-handler)
        client (wire/connect (:port server))]
    (try
      (wire/send! client "GET /park HTTP/1.1\r\nHost: localhost\r\n\r\n")
      (eventually! :shutdown-request-parked #(= 1 @fixture/parked))
      (check! :stop-reclaims-pending-snapshots 0 (:live-packets (bridge/stop! server)))
      (check! :shutdown-runs-handler-finally 0 @fixture/parked)
      (check! :stopped-port-refuses-connections :refused
              (try (let [extra (wire/connect (:port server))] (wire/close! extra) :accepted)
                   (catch Exception _ :refused)))
      (finally (wire/close! client)))))

(defn -main [& _]
  (let [reference (start-reference!)]
    (try
      (exercise! reference)
      (deadlines!)
      (shutdown!)
      (dotimes [_ 5]
        (let [server (bridge/start! #'live-handler)]
          (check! :restart-serves-request 200 (:status (wire/request! (:port server) "/plaintext")))
          (bridge/stop! server)))
      (prn {:experiment :differential :status :passed})
      (finally (stop-reference! reference) (shutdown-agents)))))
