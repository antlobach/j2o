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

(defn read-line! [process reader timeout-ms]
  ;; Jolt 0.8.27 process readers do not report BufferedReader.ready reliably.
  ;; A timed future is joined after child termination before anyone reuses reader.
  (let [task (future (.readLine reader))]
    (try
      (let [line (deref task timeout-ms ::timeout)]
        (when (= ::timeout line)
          (throw (ex-info "Child protocol read exceeded deadline" {:timeout-ms timeout-ms})))
        line)
      (catch Throwable primary
        (preserve! primary
          #(do
             (terminate! process)
             (when (= ::timeout (try (deref task 2000 ::timeout) (catch Throwable _ ::finished)))
               (throw (ex-info "Child protocol reader did not retire" {:pid (.pid process)})))))
        (throw primary)))))
