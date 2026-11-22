(ns j2o.ring-smoke
  (:require [j2o.experiments.wire :as wire]
            [j2o.native :as native]
            [jolt.fibers :as fib]
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
  (let [release (promise)
        server (adapter/run-server (fn [_] @release {:status 200 :body "late"})
                                   {:port 0 :worker-threads 1 :handler-timeout-ms 10})]
    (try
      (let [result (wire/request! (:port server) "/")]
        (equal! :deadline-status 503 (:status result))
        (equal! :deadline-body "Service Unavailable" (wire/text result)))
      (finally (deliver release true) (adapter/stop-server server)))))

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

(defn run-smoke! []
  (request-and-swap!)
  (var-redefinition!)
  (direct-fiber!)
  (deadline!)
  (oversized-failure-hook!)
  (failure-and-byte-budget!)
  (graceful-drain!)
  (println "all passed: Ring HTTP request mapping, handler swap, executor and deadline"))
