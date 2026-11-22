(ns j2o.runtime
  "Managed dispatch for the native unary transport."
  (:require [clojure.core.async :as async]
            [j2o.native :as native]
            [j2o.request :as request]
            [j2o.response :as response]
            [jolt.fibers :as fib]
            [jolt.io-poller :as poller]))

(defn- failure-response [server job error kind]
  (let [hook (:on-failure server)
        annotated (assoc (:request job) :ring-chez/failure {:kind kind})]
    (or (when (and hook (compare-and-set! (:failure-called job) false true))
          (let [result (try ((if (var? hook) @hook hook) annotated error) (catch Throwable _ nil))]
            (when (and (map? result) (contains? result :status)) result)))
        (if (= kind :timeout)
          {:status 503 :headers {"Content-Type" "text/plain"} :body "Service Unavailable"}
          {:status 500 :headers {"Content-Type" "text/plain"} :body "Internal Server Error"}))))

(defn- cancel-timer! [job]
  (when-let [timer @(:timer job)]
    (when-not (identical? timer (fib/current-fiber))
      (fib/kill! timer (ex-info "request completed" {:kind :completed})))))

(defn- submit-command! [server job command]
  (let [submitted (volatile! false)]
    (try
      (loop []
        (when (and @(:alive server) (not @(:retired job)))
          (let [outcome (try (native/response-submit! (:native server) command) :submitted
                             (catch clojure.lang.ExceptionInfo error
                               (case (:result (ex-data error))
                                 5 :full
                                 6 :stopped
                                 (throw error))))]
            (case outcome
              :submitted (vreset! submitted true)
              :full (do (Thread/sleep 1) (recur))
              :stopped nil))))
      (finally
        (when-not @submitted (native/response-release! command))))))

(defn- reply! [server job value]
  ;; Construct first: a bad body or header must not claim the terminal result.
  (let [command (response/create! (:id job) value)]
    (if (compare-and-set! (:terminal job) false true)
      (do
        (cancel-timer! job)
        (try
          (submit-command! server job command)
          (catch Throwable error
            ;; A response larger than the native byte budget cannot be queued.
            ;; Give the request one small failure response under the same claim.
            (when (and @(:alive server) (not @(:retired job)))
              (submit-command! server job
                (response/create! (:id job)
                  (failure-response server job error :crash)))))))
      (native/response-release! command))))

(defn- run-handler! [server job]
  (when-not (or @(:terminal job) @(:retired job))
    (try
      (let [target @(:handler server)
            handler (if (var? target) @target target)
            value (handler (:request job))]
        (when-not (map? value)
          (throw (ex-info "handler returned nil or a non-map" {:type :ring-chez/nil-response})))
        (reply! server job value))
      (catch Throwable error
        (when-not (or @(:terminal job) @(:retired job))
          (let [kind (if (= :ring-chez/nil-response (:type (ex-data error))) :nil-response :crash)]
            (try (reply! server job (failure-response server job error kind))
                 (catch Throwable _
                   (reply! server job {:status 500 :body "Internal Server Error"})))))))))

(defn- launch! [server f]
  (let [task (fib/spawn f {:pool (:pool server)})]
    (swap! (:fibers server) conj task)
    (fib/monitor! task (fn [_] (swap! (:fibers server) disj task)))
    task))

(defn- dispatch-request! [server event]
  (let [id (native/event-id event)
        key (vec id)
        job {:id id :terminal (atom false) :retired (atom false) :timer (atom nil)
             :task (atom nil) :failure-called (atom false)
             :request (request/from-event event (:port server) (:host server))}]
    (swap! (:requests server) assoc key job)
    (when (pos? (:timeout-ms server))
      (reset! (:timer job)
        (launch! server
          (fn []
            (Thread/sleep (:timeout-ms server))
            (when-not (or @(:terminal job) @(:retired job))
              (let [error (ex-info "handler deadline" {:type :ring-chez/handler-timeout})]
                (try (reply! server job (failure-response server job error :timeout))
                     (catch Throwable _
                       (when-not (or @(:terminal job) @(:retired job))
                         (reply! server job {:status 503 :body "Service Unavailable"}))))))))))
    (if (:handler-on-fiber server)
      (reset! (:task job) (launch! server #(run-handler! server job)))
      (when-not (async/offer! (:work server) job)
        (reply! server job {:status 503 :body "Service Unavailable"})))))

(defn- retire! [server event]
  (let [key (vec (native/event-id event))
        job (get @(:requests server) key)]
    (when job
      (reset! (:retired job) true)
      (reset! (:terminal job) true)
      (cancel-timer! job)
      (when-let [task @(:task job)]
        (fib/kill! task (ex-info "request retired" {:kind :retired})))
      (swap! (:requests server) dissoc key))))

(defn- dispatch-loop! [server]
  (while @(:running server)
    (if-let [event (native/event-take! (:native server))]
      (try
        (case (native/event-kind event)
          1 (dispatch-request! server event)
          2 (retire! server event)
          (throw (ex-info "Unknown native event" {})))
        (finally (native/event-release! event)))
      (poller/wait-ready (:fd server) :read))))

(defn- worker-loop! [server]
  (loop []
    (when-let [job (async/<!! (:work server))]
      (try (run-handler! server job) (catch Throwable _ nil))
      (recur))))

(defn start! [native-server handler opts]
  (let [pool (fib/pool {:name :j2o-dispatch :size 1})
        server {:native native-server :fd (native/event-fd native-server)
                :port (native/server-port native-server) :host (get opts :host "127.0.0.1")
                :handler (atom handler) :on-failure (:on-failure opts)
                :handler-on-fiber (boolean (:handler-on-fiber opts))
                :timeout-ms (get opts :handler-timeout-ms 0)
                :running (atom true) :alive (atom true) :requests (atom {})
                :fibers (atom #{}) :pool pool :stop-lock (Object.) :destroyed (atom false)
                :work (async/chan (get opts :max-live-requests 1024))}
        workers (atom [])]
    (try
      (when-not (:handler-on-fiber server)
        (dotimes [_ (get opts :worker-threads (.availableProcessors (Runtime/getRuntime)))]
          (let [worker (doto (Thread. #(worker-loop! server)) (.setDaemon true))]
            (.start worker)
            (swap! workers conj worker))))
      ;; A fresh OS thread has no caller bindings; its dispatch fiber and the
      ;; request fibers it spawns must not inherit the REPL's startup bindings.
      (let [result (promise)
            starter (Thread. (fn []
                               (deliver result
                                 (try [:ok (launch! server #(dispatch-loop! server))]
                                      (catch Throwable error [:error error])))))]
        (.start starter)
        (.join starter)
        (let [[status value] @result]
          (if (= status :ok)
            (assoc server :workers @workers :dispatch value)
            (throw value))))
      (catch Throwable error
        (reset! (:running server) false)
        (reset! (:alive server) false)
        (poller/cancel! (:fd server))
        (async/close! (:work server))
        (doseq [worker @workers] (.join worker))
        (doseq [task @(:fibers server)]
          (fib/kill! task (ex-info "startup failed" {:kind :stopped}))
          (try (fib/join task) (catch Throwable _ nil)))
        (poller/forget! (:fd server))
        (fib/shutdown! pool)
        (throw error)))))

(defn stop! [server drain-ms]
  (locking (:stop-lock server)
    (when-not @(:destroyed server)
      (native/begin-drain! (:native server))
      (try
        (native/join! (:native server) drain-ms)
        (catch clojure.lang.ExceptionInfo error
          (if (= "ETIMEDOUT" (:errno-name (ex-data error)))
            (do (native/force-stop! (:native server)) (native/join! (:native server) 5000))
            (throw error))))
      (reset! (:alive server) false)
      (reset! (:running server) false)
      (poller/cancel! (:fd server))
      (let [deadline (+ (System/currentTimeMillis) 5000)
            dispatch-result (try (fib/join (:dispatch server) 5000 ::timeout)
                                 (catch Throwable _ ::finished))]
        (when (= dispatch-result ::timeout)
          (throw (ex-info "Dispatcher did not stop; native state retained" {})))
        ;; The dispatcher can no longer create tasks after this point.
        (async/close! (:work server))
        (doseq [[_ job] @(:requests server)]
          (reset! (:retired job) true)
          (reset! (:terminal job) true))
        (let [tasks @(:fibers server)]
          (doseq [task tasks]
            (fib/kill! task (ex-info "server stopped" {:kind :stopped})))
          (doseq [task tasks]
            (let [result (try (fib/join task (max 0 (- deadline (System/currentTimeMillis))) ::timeout)
                              (catch Throwable _ ::finished))]
              (when (= result ::timeout)
                (throw (ex-info "Fiber did not stop; native state retained" {}))))))
        (doseq [worker (:workers server)]
          (.join worker (max 1 (- deadline (System/currentTimeMillis))))
          (when (.isAlive worker)
            (throw (ex-info "Worker did not stop; native state retained" {})))))
      (poller/forget! (:fd server))
      (native/server-destroy! (:native server))
      (fib/shutdown! (:pool server))
      (reset! (:destroyed server) true)))
  nil)
