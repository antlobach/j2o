(ns j2o.experiments.bench-server
  (:require [j2o.experiments.bridge :as bridge]))

(defn handler [_] {:status 200 :body "Hello, World!\n"})

(defn -main [& _]
  (when-not (= "true" (System/getenv "GITHUB_ACTIONS"))
    (throw (ex-info "Performance measurements run on GitHub runners only" {})))
  (let [server (bridge/start! #'handler)]
    (println "READY" (:port server))
    (flush)
    (try
      (read-line)
      (finally
        (prn {:stopped (bridge/stop! server)})
        (shutdown-agents)))))
