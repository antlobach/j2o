(ns j2o.experiments.bridge
  (:require [clojure.string :as str]
            [jolt.ffi :as ffi]
            [jolt.fibers :as fib]
            [jolt.io-poller :as poller]))

(ffi/load-library
  (str (System/getProperty "user.dir") "/.cache/libj2o_probe."
       (if (str/includes? (System/getProperty "os.name") "Mac") "dylib" "so")))

;; :blocking is Chez's collect-safe transition, not a fiber parking operation.
;; Every call that waits, locks, allocates or copies variable-sized data uses it.
;; Arguments are native allocations, never pointers into movable managed objects.
(ffi/defcfn native-start "j2o_probe_start" [] :pointer :blocking)
(ffi/defcfn native-port "j2o_probe_port" [:pointer] :int)
(ffi/defcfn native-fd "j2o_probe_fd" [:pointer] :int)
(ffi/defcfn native-take "j2o_probe_take" [:pointer] :pointer :blocking)
(ffi/defcfn native-id "j2o_probe_id" [:pointer] :uint64)
(ffi/defcfn native-version "j2o_probe_version" [:pointer] :int)
(ffi/defcfn native-field "j2o_probe_field" [:pointer :int :pointer] :pointer)
(ffi/defcfn native-release "j2o_probe_release" [:pointer :pointer] :void :blocking)
(ffi/defcfn native-respond "j2o_probe_respond" [:pointer :uint64 :int :pointer :size_t] :int :blocking)
(ffi/defcfn native-stat "j2o_probe_stat" [:pointer :int] :uint64)
(ffi/defcfn native-stop "j2o_probe_stop" [:pointer] :void :blocking)
(ffi/defcfn native-destroy "j2o_probe_destroy" [:pointer] :void :blocking)
(ffi/defcfn native-hold "j2o_probe_hold" [:uint] :void :blocking)
(ffi/defcfn unsafe-native-hold "j2o_probe_hold" [:uint] :void)
(ffi/defcfn native-hold-active "j2o_probe_hold_active" [] :int)

(defn read-field [packet field bytes?]
  (ffi/with-out [size :size_t]
    (let [data (native-field packet field size)
          n (ffi/read size :size_t)]
      (if bytes? (ffi/read-array data n) (ffi/read-bytes data n)))))

(defn take-request [native port]
  (let [packet (native-take native)]
    (when-not (ffi/null? packet)
      (try
        (let [target (read-field packet 1 false)
              query (.indexOf target "?")
              bytes (read-field packet 2 true)]
          {:j2o.experiment/id (native-id packet)
           :request-method (keyword (str/lower-case (read-field packet 0 false)))
           :uri (if (neg? query) target (subs target 0 query))
           :query-string (when-not (neg? query) (subs target (inc query)))
           :protocol (case (native-version packet)
                       256 "HTTP/1.0" 257 "HTTP/1.1" 512 "HTTP/2")
           :scheme :http :server-port port :server-name "127.0.0.1"
           :remote-addr "127.0.0.1"
           :body (when (pos? (alength bytes)) (java.io.ByteArrayInputStream. bytes))})
        (finally (native-release native packet))))))

(defn respond! [native id response]
  (let [body (:body response)
        bytes (cond
                (nil? body) (byte-array 0)
                (string? body) (.getBytes body "UTF-8")
                (bytes? body) body
                :else (throw (ex-info "Probe supports string/byte-array bodies only" {})))]
    (ffi/with-alloc [data (max 1 (alength bytes))]
      (ffi/write-array data bytes)
      (when-not (zero? (native-respond native id (:status response) data (alength bytes)))
        (throw (ex-info "Native response submission failed" {:id id}))))))

(defn stats [server]
  (into {} (map-indexed (fn [i k] [k (native-stat (:native server) i)])
                       [:admitted :answered :stale :disposed :rejected :live-packets])))

(defn start!
  ([handler] (start! handler {}))
  ([handler {:keys [timeout-ms] :or {timeout-ms 0}}]
   (when-not (or (fn? handler) (var? handler))
     (throw (ex-info "Handler must be a function or Var" {})))
   (let [native (native-start)]
     (when (ffi/null? native) (throw (ex-info "Native H2O startup failed" {})))
     (let [port (native-port native)
           fd (native-fd native)
           pool (fib/pool {:name :j2o-probe :size 1})
           running (atom true)
           handlers (atom {})
           supervisors (atom {})
           registered (atom handler)
           dispatch
           (fib/spawn
             (fn []
               (while @running
                 (if-let [request (take-request native port)]
                   (let [id (:j2o.experiment/id request)
                         terminal (atom false)
                         reply (fn [response]
                                 (when (compare-and-set! terminal false true)
                                   (respond! native id response)
                                   true))
                         task (fib/spawn
                                (fn []
                                  (try
                                    (let [target @registered
                                          f (if (var? target) @target target)
                                          response (f request)]
                                      (when-not (and (map? response)
                                                     (integer? (:status response))
                                                     (<= 200 (:status response) 599)
                                                     (or (nil? (:body response))
                                                         (string? (:body response))
                                                         (bytes? (:body response))))
                                        (throw (ex-info "Invalid probe Ring response" {})))
                                      (reply response))
                                    (catch Throwable _
                                      (reply {:status 500 :body "handler failed"}))))
                                {:pool pool})]
                     (swap! handlers assoc id task)
                     (fib/monitor! task (fn [_] (swap! handlers dissoc id)))
                     (when (pos? timeout-ms)
                       (let [supervisor
                             (fib/spawn
                               (fn []
                                 (when (= ::timeout (fib/join task timeout-ms ::timeout))
                                   (when (reply {:status 503 :body "handler deadline"})
                                     (fib/kill! task (ex-info "handler deadline" {:kind :timeout})))))
                               {:pool pool})]
                         (swap! supervisors assoc id supervisor)
                         (fib/monitor! supervisor (fn [_] (swap! supervisors dissoc id))))))
                   (poller/wait-ready fd :read))))
             {:pool pool})]
       {:native native :port port :fd fd :pool pool :running running
        :handlers handlers :supervisors supervisors :registered registered :dispatch dispatch}))))

(defn stop! [server]
  (reset! (:running server) false)
  (poller/cancel! (:fd server))
  (when (= ::timeout (fib/join (:dispatch server) 3000 ::timeout))
    (throw (ex-info "Dispatcher did not stop" {})))
  (doseq [task (concat (vals @(:handlers server)) (vals @(:supervisors server)))]
    (fib/kill! task (ex-info "server stopped" {:kind :stopped}))
    (let [result (try (fib/join task 3000 ::timeout) (catch Throwable _ ::finished))]
      (when (= result ::timeout)
        (throw (ex-info "Request fiber did not stop; retaining native state" {})))))
  (native-stop (:native server))
  (let [result (stats server)]
    (poller/forget! (:fd server))
    (native-destroy (:native server))
    (fib/shutdown! (:pool server))
    result))
