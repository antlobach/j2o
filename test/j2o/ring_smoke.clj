(ns j2o.ring-smoke
  (:require [j2o.experiments.wire :as wire]
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

(defn- direct-fiber! []
  (let [server (adapter/run-server (fn [_] {:status 200 :body (str (fib/in-fiber?))})
                                   {:port 0 :strategy :fibers :handler-on-fiber true})]
    (try
      (equal! :opt-in-fiber "true" (wire/text (wire/request! (:port server) "/")))
      (finally (adapter/stop-server server)))))

(defn- deadline! []
  (let [server (adapter/run-server (fn [_] (Thread/sleep 100) {:status 200 :body "late"})
                                   {:port 0 :worker-threads 1 :handler-timeout-ms 10})]
    (try
      (let [result (wire/request! (:port server) "/")]
        (equal! :deadline-status 503 (:status result))
        (equal! :deadline-body "Service Unavailable" (wire/text result)))
      (finally (adapter/stop-server server)))))

(defn run! []
  (request-and-swap!)
  (direct-fiber!)
  (deadline!)
  (println "all passed: Ring HTTP request mapping, handler swap, executor and deadline"))
