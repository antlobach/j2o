(ns ring-chez.adapter
  "Ring server entry points backed by the j2o H2O transport."
  (:require [j2o.native :as native]
            [j2o.runtime :as runtime]
            [jolt.io-poller :as poller]))

(def ^:private numeric-options
  {:port [2 0 65535]
   :max-request-bytes [4 1 Long/MAX_VALUE]
   :max-header-bytes [5 1 Long/MAX_VALUE]
   :max-live-requests [6 1 4294967295]
   :max-event-queue [7 1 Long/MAX_VALUE]
   :max-response-queue [8 1 Long/MAX_VALUE]
   :max-queued-bytes [9 1 Long/MAX_VALUE]
   :max-connections [10 1 Long/MAX_VALUE]
   :keep-alive-timeout-ms [11 1 Long/MAX_VALUE]
   :request-timeout-ms [12 0 Long/MAX_VALUE]
   :write-timeout-ms [13 0 Long/MAX_VALUE]
   :handshake-timeout-ms [14 1 Long/MAX_VALUE]})

(defn- validate! [handler opts]
  (when-not (or (fn? handler) (var? handler))
    (throw (ex-info "Handler must be a function or Var" {:given handler})))
  (when-not (map? opts)
    (throw (ex-info "Server options must be a map" {:given opts})))
  (let [strategy (get opts :strategy :threads)]
    (when-not (contains? #{:threads :fibers} strategy)
      (throw (ex-info "Invalid server strategy" {:key :strategy :given strategy})))
    (when-not (poller/available?)
      (throw (ex-info "j2o requires a supported readiness poller" {:key :strategy :given strategy})))
    (when (and (:handler-on-fiber opts) (not= strategy :fibers))
      (throw (ex-info ":handler-on-fiber requires :strategy :fibers" {:key :handler-on-fiber})))
    (when (and (:handler-on-fiber opts) (pos? (get opts :handler-timeout-ms 0)))
      (throw (ex-info ":handler-on-fiber cannot be combined with a handler deadline" {:key :handler-on-fiber}))))
  (doseq [[key [_ minimum maximum]] numeric-options :when (contains? opts key)]
    (let [value (get opts key)]
      (when-not (and (integer? value) (<= minimum value maximum))
        (throw (ex-info "Invalid server option" {:key key :given value})))))
  (doseq [key [:worker-threads :handler-timeout-ms] :when (contains? opts key)]
    (let [value (get opts key) minimum (if (= key :worker-threads) 1 0)]
      (when-not (and (integer? value) (<= minimum value Integer/MAX_VALUE))
        (throw (ex-info "Invalid server option" {:key key :given value})))))
  (doseq [key [:reuse-port :handler-on-fiber] :when (contains? opts key)]
    (when-not (boolean? (get opts key))
      (throw (ex-info "Expected a boolean server option" {:key key :given (get opts key)}))))
  (when (and (contains? opts :host) (not (and (string? (:host opts)) (seq (:host opts)))))
    (throw (ex-info "Expected a non-empty host" {:key :host :given (:host opts)})))
  (when (and (contains? opts :on-failure) (not (fn? (:on-failure opts))))
    (throw (ex-info "Expected an on-failure function" {:key :on-failure :given (:on-failure opts)})))
  (doseq [key [:tls :ws-handler :ws-guard] :when (contains? opts key)]
    (throw (ex-info "Transport option is not yet available" {:key key :given (get opts key)}))))

(defn run-server [handler opts]
  (validate! handler opts)
  (let [config (native/config-create!)]
    (try
      (when (contains? opts :host) (native/config-string! config 1 (:host opts)))
      (when (contains? opts :reuse-port) (native/config-bool! config 3 (:reuse-port opts)))
      (doseq [[key [id]] numeric-options :when (contains? opts key)]
        (native/config-u64! config id (get opts key)))
      (let [server (native/server-start! config)]
        (try
          (runtime/start! server handler opts)
          (catch Throwable error
            (native/force-stop! server)
            (native/join! server 5000)
            (native/server-destroy! server)
            (throw error))))
      (finally (native/config-destroy! config)))))

(defn stop-server
  ([server] (stop-server server {}))
  ([server opts]
   (let [timeout (get opts :drain-timeout-ms 5000)]
     (when-not (and (integer? timeout) (<= 0 timeout Integer/MAX_VALUE))
       (throw (ex-info "Invalid drain timeout" {:key :drain-timeout-ms :given timeout})))
     (runtime/stop! server timeout))))

(defn swap-handler! [server handler]
  (when-not (or (fn? handler) (var? handler))
    (throw (ex-info "Handler must be a function or Var" {:given handler})))
  (reset! (:handler server) handler)
  server)
