(ns j2o.ring-smoke
  (:require [clojure.string :as str]
            [j2o.experiments.wire :as wire]
            [j2o.native :as native]
            [jolt.fibers :as fib]
            [jolt.scheme :as scheme]
            [ring-chez.adapter :as adapter]))

(defn- equal! [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str "Failed: " label) {:expected expected :given actual}))))

(defn- request-and-swap! []
  (let [seen (atom nil)
        server (adapter/run-server
                 (fn [request]
                   (reset! seen (select-keys request [:uri :ring-chez/raw-uri :query-string
                                                     :server-name :request-method :protocol]))
                   {:status 201 :headers {"X-Reply" "first"}
                    :body (str (fib/in-fiber?))})
                 {:port 0 :worker-threads 2})
        client (wire/connect (:port server))]
    (try
      (wire/send! client "GET /a//b/../c?q=1 HTTP/1.1\r\nHost: example.test:1234\r\n\r\n")
      (let [result (wire/response! client)]
        (equal! :status 201 (:status result))
        (equal! :worker-not-fiber "false" (wire/text result))
        (equal! :response-header "first" (get-in result [:headers "x-reply"]))
        (equal! :request-map {:uri "/a/c" :ring-chez/raw-uri "/a//b/../c" :query-string "q=1"
                              :server-name "example.test" :request-method :get :protocol "HTTP/1.1"}
                @seen))
      (adapter/swap-handler! server (fn [_] {:status 200 :body "second"}))
      (wire/send! client "GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
      (equal! :same-connection-swap "second" (wire/text (wire/response! client)))
      (finally
        (wire/close! client)
        (adapter/stop-server server)))))

(defn- live-handler [_] {:status 200 :body "before"})

(defn- var-redefinition! []
  (let [original @#'live-handler
        server (adapter/run-server #'live-handler {:port 0 :worker-threads 1})
        client (wire/connect (:port server))
        request "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n"]
    (try
      (wire/send! client request)
      (equal! :var-before "before" (wire/text (wire/response! client)))
      (alter-var-root #'live-handler (constantly (fn [_] {:status 200 :body "after"})))
      (wire/send! client request)
      (equal! :var-after "after" (wire/text (wire/response! client)))
      (finally
        (alter-var-root #'live-handler (constantly original))
        (wire/close! client)
        (adapter/stop-server server)))))

(defn- direct-fiber! []
  (let [server (adapter/run-server (fn [_] {:status 200 :body (str (fib/in-fiber?))})
                                   {:port 0 :strategy :fibers :handler-on-fiber true})]
    (try
      (equal! :opt-in-fiber "true" (wire/text (wire/request! (:port server) "/")))
      (finally (adapter/stop-server server)))))

(defn- deadline! []
  (let [release (promise) entered (promise) finished (promise)
        server (adapter/run-server (fn [_] (deliver entered true)
                                     (try @release {:status 200 :body "late"}
                                          (finally (deliver finished true))))
                                   {:port 0 :worker-threads 1 :handler-timeout-ms 100})
        client (wire/connect (:port server))]
    (try
      (wire/send! client "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
      (equal! :deadline-handler-entered true (deref entered 5000 false))
      (let [result (wire/response! client)]
        (equal! :deadline-status 503 (:status result))
        (equal! :deadline-body "Service Unavailable" (wire/text result)))
      (deliver release true)
      (equal! :late-handler-finished true (deref finished 5000 false))
      (adapter/swap-handler! server (fn [_] {:status 200 :body "next"}))
      (wire/send! client "GET /next HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
      (equal! :late-response-isolated "next" (wire/text (wire/response! client)))
      (finally (deliver release true) (wire/close! client) (adapter/stop-server server)))))

(defn- compute-deadline! []
  (let [entered (promise) finished (promise) stop (atom false)
        server (adapter/run-server
                 (fn [_] (deliver entered true)
                   (try (loop [] (when-not @stop (recur))) {:status 200 :body "late"}
                        (finally (deliver finished true))))
                 {:port 0 :worker-threads 1 :handler-timeout-ms 100})
        client (wire/connect (:port server))]
    (try
      (wire/send! client "GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
      (equal! :compute-handler-entered true (deref entered 5000 false))
      (equal! :compute-deadline 503 (:status (wire/response! client)))
      (equal! :interrupted-finally true (deref finished 5000 false))
      (finally (reset! stop true) (wire/close! client) (adapter/stop-server server)))))

(defn- failure-and-byte-budget! []
  (let [calls (atom 0)
        server (adapter/run-server
                 (fn [_] {:status 200 :body (apply str (repeat 1024 "x"))})
                 {:port 0 :worker-threads 1 :max-queued-bytes 256
                  :on-failure (fn [_ _] (swap! calls inc) {:status 501 :body "too large"})})]
    (try
      (let [result (wire/request! (:port server) "/")]
        (equal! :oversize-status 501 (:status result))
        (equal! :oversize-body "too large" (wire/text result))
        (equal! :failure-hook-once 1 @calls))
      (finally (adapter/stop-server server)))))

(defn- oversized-failure-hook! []
  (let [calls (atom 0)
        large (apply str (repeat 1024 "x"))
        server (adapter/run-server (fn [_] {:status 200 :body large})
                 {:port 0 :worker-threads 1 :max-queued-bytes 128
                  :on-failure (fn [_ _] (swap! calls inc) {:status 501 :body large})})]
    (try
      (let [result (wire/request! (:port server) "/")]
        (equal! :oversized-hook-fallback 500 (:status result))
        (equal! :oversized-hook-once 1 @calls))
      (finally (adapter/stop-server server)))))

(defn- unsafe-response-header! []
  (let [server (adapter/run-server
                 (fn [_] {:status 200 :headers {"X-Safe" "ok"
                                                "X-Unsafe" "value\r\nX-Injected: yes"
                                                "Content-Length" "999"}
                          :body "safe"})
                 {:port 0 :worker-threads 1})]
    (try
      (let [result (wire/request! (:port server) "/")]
        (equal! :safe-header "ok" (get-in result [:headers "x-safe"]))
        (equal! :unsafe-header-dropped nil (get-in result [:headers "x-unsafe"]))
        (equal! :injected-header-absent nil (get-in result [:headers "x-injected"]))
        (equal! :framing-owned-by-transport "4" (get-in result [:headers "content-length"]))
        (equal! :safe-body "safe" (wire/text result)))
      (finally (adapter/stop-server server)))))

(defn- gc-request-body! []
  (let [collect! (scheme/eval-string "(lambda () (let ([n (collections)]) (collect-rendezvous) (- (collections) n)))")
        collected (atom 0)
        server (adapter/run-server
                 (fn [request]
                   (let [body (.readAllBytes (:body request))]
                     (reset! collected (collect!))
                     {:status 200 :body body}))
                 {:port 0 :worker-threads 1})
        body (byte-array [0 1 -1 -128 42])]
    (try
      (equal! :binary-body-through-gc (vec body)
              (vec (:body (wire/request! (:port server) "POST" "/" body))))
      (equal! :collection-progress true (pos? @collected))
      (finally (adapter/stop-server server)))))

(defn- graceful-drain! []
  (let [entered (promise) release (promise)
        server (adapter/run-server (fn [_] (deliver entered true) @release {:status 200 :body "drained"})
                                   {:port 0 :worker-threads 1})
        client (wire/connect (:port server))]
    (try
      (wire/send! client "GET / HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
      (equal! :handler-entered true (deref entered 5000 false))
      (equal! :active-stats {:connections 1 :requests 1 :active 1 :faults 0}
              (select-keys (adapter/server-stats server) [:connections :requests :active :faults]))
      (native/begin-drain! (:native server))
      (let [stopped (future (adapter/stop-server server))]
        (deliver release true)
        (equal! :drain-response "drained" (wire/text (wire/response! client)))
        (equal! :drain-completed nil (deref stopped 5000 ::timeout))
        (equal! :stopped-stats {:connections 0 :requests 1 :active 0 :faults 0}
                (select-keys (adapter/server-stats server) [:connections :requests :active :faults])))
      (finally
        (deliver release true)
        (wire/close! client)
        (adapter/stop-server server)))))

(defn- http2-header-policy! []
  (let [server (adapter/run-server
                 (fn [request] {:status 200 :headers {"Connection" "close"
                                                      "Keep-Alive" "timeout=5"
                                                      "X-Safe" "ok"}
                                :body (:protocol request)})
                 {:port 0 :worker-threads 1})]
    (try
      (let [process (.start (ProcessBuilder.
                              ["/usr/bin/curl" "--http2-prior-knowledge" "--fail"
                               "--silent" "--show-error" "--max-time" "5" "--dump-header" "-"
                               (str "http://127.0.0.1:" (:port server) "/")]))
            output (slurp (.getInputStream process))
            error (slurp (.getErrorStream process))
            exit (.waitFor process)
            normalized (str/lower-case output)]
        (when-not (zero? exit)
          (throw (ex-info "HTTP/2 client rejected response" {:exit exit :error error})))
        (equal! :http2-handler-protocol true (str/ends-with? output "HTTP/2"))
        (equal! :http2-safe-header true (str/includes? normalized "x-safe: ok"))
        (equal! :http2-no-connection-header false (str/includes? normalized "\nconnection:"))
        (equal! :http2-no-keep-alive-header false (str/includes? normalized "\nkeep-alive:")))
      (finally (adapter/stop-server server)))))

(defn run-smoke! []
  (request-and-swap!)
  (var-redefinition!)
  (direct-fiber!)
  (deadline!)
  (compute-deadline!)
  (oversized-failure-hook!)
  (failure-and-byte-budget!)
  (unsafe-response-header!)
  (http2-header-policy!)
  (gc-request-body!)
  (graceful-drain!)
  (println "all passed: Ring HTTP request mapping, handler swap, executor and deadline"))
