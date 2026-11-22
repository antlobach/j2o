(ns j2o.experiments.bridge
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [j2o.experiments.profiling :as profiling]
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
(ffi/defcfn native-ingress-mode "j2o_probe_ingress_mode" [:pointer :int] :int :blocking)
(ffi/defcfn native-await "j2o_probe_await" [:pointer] :pointer :blocking)
(ffi/defcfn native-interrupt-ingress "j2o_probe_interrupt_ingress" [:pointer] :void :blocking)
(ffi/defcfn native-id "j2o_probe_id" [:pointer] :uint64)
(ffi/defcfn native-version "j2o_probe_version" [:pointer] :int)
(ffi/defcfn native-field "j2o_probe_field" [:pointer :int :pointer] :pointer)
(ffi/defcfn native-release "j2o_probe_release" [:pointer :pointer] :void :blocking)
(ffi/defcfn native-response-create "j2o_probe_response_create" [:uint64 :int :pointer :size_t :pointer] :pointer :blocking)
(ffi/defcfn native-response-submit "j2o_probe_response_submit" [:pointer :pointer] :int :blocking)
(ffi/defcfn native-response-release "j2o_probe_response_release" [:pointer] :void :blocking)
(ffi/defcfn native-response-limit "j2o_probe_response_limit" [] :size_t)
(ffi/defcfn native-stat "j2o_probe_stat" [:pointer :int] :uint64)
(ffi/defcfn native-stop "j2o_probe_stop" [:pointer] :void :blocking)
(ffi/defcfn native-destroy "j2o_probe_destroy" [:pointer] :void :blocking)
(ffi/defcfn native-hold "j2o_probe_hold" [:uint] :void :blocking)
(ffi/defcfn unsafe-native-hold "j2o_probe_hold" [:uint] :void)
(ffi/defcfn native-hold-active "j2o_probe_hold_active" [] :int)

(def max-response-bytes (native-response-limit))

(defn read-field-into [packet field bytes? size]
  (let [data (native-field packet field size)
        n (ffi/read size :size_t)]
    (if bytes? (ffi/read-array data n) (ffi/read-bytes data n))))

(defn read-field [packet field bytes?]
  (ffi/with-out [size :size_t]
    (read-field-into packet field bytes? size)))

(defn take-request [native port observer receiver scratch]
  (let [packet (if receiver (async/<!! (:packets receiver)) (native-take native))]
    (when-not (or (nil? packet) (ffi/null? packet))
      (try
        (when observer (profiling/decoding! observer packet))
        (let [target (if scratch (read-field-into packet 1 false scratch) (read-field packet 1 false))
              query (.indexOf target "?")
              bytes (if scratch (read-field-into packet 2 true scratch) (read-field packet 2 true))]
          {:j2o.experiment/id (native-id packet)
           :request-method (keyword (str/lower-case
                                      (if scratch (read-field-into packet 0 false scratch) (read-field packet 0 false))))
           :uri (if (neg? query) target (subs target 0 query))
           :query-string (when-not (neg? query) (subs target (inc query)))
           :protocol (case (native-version packet)
                       256 "HTTP/1.0" 257 "HTTP/1.1" 512 "HTTP/2")
           :scheme :http :server-port port :server-name "127.0.0.1"
           :remote-addr "127.0.0.1"
           :body (when (pos? (alength bytes)) (java.io.ByteArrayInputStream. bytes))})
        (finally (native-release native packet))))))

(defn body-bytes [body]
  (let [bytes (cond
                (nil? body) (byte-array 0)
                (string? body) (.getBytes body "UTF-8")
                (bytes? body) body
                :else (throw (ex-info "Probe supports string/byte-array bodies only" {})))]
    (when (> (alength bytes) max-response-bytes)
      (throw (ex-info "Probe response exceeds native buffer limit" {})))
    bytes))

(defn respond-bytes! [native id status bytes observer trace terminal]
  (let [message (ffi/with-alloc [data (max 1 (alength bytes))]
                  (ffi/write-array data bytes)
                  (when observer (profiling/mark! observer trace :submit-ready))
                  (native-response-create id status data (alength bytes) trace))]
    (when (ffi/null? message)
      (throw (ex-info "Native response preparation failed" {:id id})))
    (let [submitted (volatile! false)]
      (try
        (when (compare-and-set! terminal false true)
          (when-not (zero? (native-response-submit native message))
            (throw (ex-info "Native response submission failed" {:id id})))
          (vreset! submitted true)
          true)
        (finally
          (when-not @submitted (native-response-release message)))))))

(defn respond! [native id response]
  (respond-bytes! native id (:status response) (body-bytes (:body response)) nil 0 (atom false)))

(defn stats [server]
  (into {} (map-indexed (fn [i k] [k (native-stat (:native server) i)])
                       [:admitted :answered :stale :disposed :rejected :live-packets :connections])))

(defn start-ingress! [native]
  (let [packets (async/chan 1)
        done (async/thread
               (try
                 (loop []
                   (let [packet (native-await native)]
                     (when-not (ffi/null? packet)
                       (let [transferred (volatile! false)]
                         (try
                           (vreset! transferred (boolean (async/>!! packets packet)))
                           (finally
                             (when-not @transferred (native-release native packet))))
                         (when @transferred (recur))))))
                 [:ok]
                 (catch Throwable t [:error t])
                 (finally (async/close! packets))))]
    {:packets packets :done done}))

(defn release-runtime! [server]
  (let [{:keys [native running receiver fd dispatch handlers supervisors pool]} server]
    (reset! running false)
    (if receiver
      (do (native-interrupt-ingress native)
          (async/close! (:packets receiver)))
      (poller/cancel! fd))
    (when (and dispatch (= ::timeout (fib/join dispatch 3000 ::timeout)))
      (throw (ex-info "Dispatcher did not stop; retaining native state" {})))
    (let [receiver-error
          (when receiver
            (let [[result channel] (async/alts!! [(:done receiver) (async/timeout 3000)])]
              (when-not (= channel (:done receiver))
                (throw (ex-info "Ingress thread did not stop; retaining native state" {})))
              (loop []
                (when-let [packet (async/poll! (:packets receiver))]
                  (native-release native packet)
                  (recur)))
              (when (= :error (first result)) (second result))))]
      (doseq [task (concat (vals @handlers) (vals @supervisors))]
        (fib/kill! task (ex-info "server stopped" {:kind :stopped}))
        (let [result (try (fib/join task 3000 ::timeout) (catch Throwable _ ::finished))]
          (when (= result ::timeout)
            (throw (ex-info "Request fiber did not stop; retaining native state" {})))))
      (native-stop native)
      (let [result (stats server)]
        (poller/forget! fd)
        (native-destroy native)
        (when pool (fib/shutdown! pool))
        (when receiver-error (throw receiver-error))
        result))))

(defn start!
  ([handler] (start! handler {}))
  ([handler {:keys [timeout-ms stage-profile ingress scratch?]
             :or {timeout-ms 0 ingress (keyword (or (System/getenv "J2O_PROBE_INGRESS") "poller"))
                  scratch? (= "1" (System/getenv "J2O_PROBE_SCRATCH"))}}]
   (when-not (or (fn? handler) (var? handler))
     (throw (ex-info "Handler must be a function or Var" {})))
   (when-not (contains? #{:poller :waiter} ingress)
     (throw (ex-info "Unknown experimental ingress" {:ingress ingress})))
   (let [native (native-start)]
     (when (ffi/null? native) (throw (ex-info "Native H2O startup failed" {})))
     (let [resources (atom {:native native :port (native-port native) :fd (native-fd native)
                            :running (atom true) :handlers (atom {}) :supervisors (atom {})})
           {:keys [port fd running handlers supervisors]} @resources]
       (try
         (when-not (zero? (native-ingress-mode native (if (= :waiter ingress) 1 0)))
           (throw (ex-info "Native ingress selection failed" {:ingress ingress})))
         (let [pool (fib/pool {:name :j2o-probe :size 1})
               _ (swap! resources assoc :pool pool)
               observer (when stage-profile (profiling/create! native stage-profile))
               _ (swap! resources assoc :profile observer)
               registered (atom handler)
               receiver (when (= :waiter ingress) (start-ingress! native))
               _ (swap! resources assoc :receiver receiver :registered registered :ingress ingress)
               dispatch
           (fib/spawn
             (fn []
               (ffi/with-out [field-size :size_t]
               (while @running
                 (if-let [request (take-request native port observer receiver (when scratch? field-size))]
                   (let [id (:j2o.experiment/id request)
                         trace (if observer @(:last-trace observer) 0)
                         _ (when observer (profiling/mark! observer trace :request-decoded))
                         terminal (atom false)
                         reply (fn [response]
                                 (let [bytes (body-bytes (:body response))]
                                   (when observer (profiling/mark! observer trace :response-encoded))
                                   (respond-bytes! native id (:status response) bytes observer trace terminal)))
                         task (do
                                (when observer (profiling/mark! observer trace :fiber-enqueued))
                                (fib/spawn
                                (fn []
                                  (when observer (profiling/mark! observer trace :handler-start))
                                  (try
                                    (let [target @registered
                                          f (if (var? target) @target target)
                                          response (f request)]
                                      (when observer (profiling/mark! observer trace :handler-return))
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
                                {:pool pool}))]
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
                   (if receiver
                     (when @running (throw (ex-info "Ingress thread exited unexpectedly" {})))
                     (poller/wait-ready fd :read))))))
             {:pool pool})]
           (swap! resources assoc :dispatch dispatch))
         (catch Throwable primary
           (try (release-runtime! @resources)
                (catch Throwable cleanup
                  ;; Jolt has no Throwable.addSuppressed; retain the original cause.
                  (throw (ex-info (ex-message primary)
                                  (assoc (or (ex-data primary) {}) :cleanup-errors [cleanup])
                                  primary))))
           (throw primary)))))))

(defn stop! [server]
  (release-runtime! server))
