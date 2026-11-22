(ns j2o.experiments.ring-comparison-server
  (:require [ring-chez.adapter :as adapter]))

(when-not (= "true" (System/getenv "GITHUB_ACTIONS"))
  (throw (ex-info "Benchmarks run on GitHub-hosted runners only" {})))

;; Loaded from the pinned reference checkout's working directory and deps.edn.
;; No j2o adapter, bridge or modified reference source is on this classpath.
(let [variant (System/getenv "RING_BENCH_VARIANT")
      body (.getBytes (slurp (System/getenv "RING_BENCH_BODY")) "UTF-8")
      response {:status 200 :headers {"Content-Type" "text/plain"} :body body}
      opts (merge {:host "127.0.0.1" :port 0}
                  (case variant
                    "ring-default" {}
                    "ring-threads-64" {:strategy :threads :worker-threads 64}
                    "ring-fibers" {:strategy :fibers}
                    "ring-fibers-direct" {:strategy :fibers :handler-on-fiber true}))
      server (adapter/run-server (fn [_] response) opts)]
  (println "READY" (:port server))
  (prn {:variant variant :options opts
        :available-processors (.availableProcessors (Runtime/getRuntime))
        :response-bytes (alength body)})
  (flush)
  (try
    (when-not (= "stop" (read-line))
      (throw (ex-info "Expected benchmark shutdown command" {})))
    (finally
      (adapter/stop-server server)
      (println "STOPPED")
      (flush)
      (shutdown-agents))))
