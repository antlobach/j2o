(ns j2o.experiments.http2
  (:require [clojure.string :as str]
            [j2o.experiments.bridge :as bridge]
            [j2o.experiments.server :as fixture]
            [j2o.experiments.differential :as differential]))

(defn curl! [port path]
  (let [process (.start
                  (ProcessBuilder.
                    ["curl" "--http2-prior-knowledge" "--fail" "--silent" "--show-error"
                     "--max-time" "5" "--write-out" "\nHTTP_VERSION=%{http_version}\n"
                     (str "http://127.0.0.1:" port path)]))
        output (slurp (.getInputStream process))
        error (slurp (.getErrorStream process))
        exit (.waitFor process)]
    (when-not (zero? exit)
      (throw (ex-info "HTTP/2 client failed" {:exit exit :error error})))
    (when-not (str/ends-with? output "HTTP_VERSION=2\n")
      (throw (ex-info "Client did not negotiate HTTP/2" {:output output})))
    output))

(defn -main [& _]
  (let [reference (differential/start-reference!)
        server (bridge/start! #'fixture/handler)]
    (try
      (differential/check! :native-reference-http2
                           (curl! (:port reference) "/plaintext")
                           (curl! (:port server) "/plaintext"))
      (differential/check! :ring-observes-http2 "HTTP/2\nHTTP_VERSION=2\n"
                           (curl! (:port server) "/version"))
      (prn {:experiment :http2 :status :passed})
      (finally (bridge/stop! server)
               (differential/stop-reference! reference)
               (shutdown-agents)))))
