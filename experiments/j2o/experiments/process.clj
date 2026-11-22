(ns j2o.experiments.process
  (:import [java.util.concurrent TimeUnit]))

(defn preserve! [primary cleanup]
  (try (cleanup)
       (catch Throwable error
         (if primary
           ;; Jolt cannot mutate Throwable's suppressed list.
           (throw (ex-info (ex-message primary)
                           (assoc (or (ex-data primary) {}) :cleanup-errors [error]) primary))
           (throw error)))))

(defn terminate! [process]
  (let [errors (atom [])
        attempt (fn [f] (try (f) (catch Throwable error (swap! errors conj error) nil)))]
    (attempt #(.destroy process))
    (when-not (attempt #(.waitFor process 2000 TimeUnit/MILLISECONDS))
      (attempt #(.destroyForcibly process))
      (when-not (attempt #(.waitFor process 2000 TimeUnit/MILLISECONDS))
        (swap! errors conj (ex-info "Child survived forced termination" {:pid (.pid process)}))))
    (when (seq @errors)
      (throw (ex-info "Child termination failed" {:cleanup-errors @errors} (first @errors))))))

(defn await! [process timeout-ms]
  (try
    (when-not (.waitFor process timeout-ms TimeUnit/MILLISECONDS)
      (throw (ex-info "Child exceeded deadline" {:pid (.pid process) :timeout-ms timeout-ms})))
    (.exitValue process)
    (catch Throwable primary
      (preserve! primary #(terminate! process))
      (throw primary))))

(defn read-line! [reader timeout-ms]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))
        text (StringBuilder.)]
    (loop []
      (when (>= (System/nanoTime) deadline)
        (throw (ex-info "Child protocol read exceeded deadline" {:timeout-ms timeout-ms})))
      (if (.ready reader)
        (let [value (.read reader)]
          (cond
            (= -1 value) (when (pos? (.length text)) (str text))
            (= 10 value) (str text)
            (= 13 value) (recur)
            :else (do (.append text (char value)) (recur))))
        (do (Thread/sleep 1) (recur))))))
