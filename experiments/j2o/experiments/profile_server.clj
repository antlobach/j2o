(ns j2o.experiments.profile-server
  (:require [j2o.experiments.bridge :as bridge]
            [j2o.experiments.differential :as checks]
            [j2o.experiments.profiling :as profiling]
            [jolt.profile :as profile]))

(defn handler [_] {:status 200 :body "Hello, World!\n"})

(defn quiesce! [server]
  (checks/eventually!
    :profile-quiescence
    #(let [stats (bridge/stats server)]
       (and (= (:admitted stats) (+ (:answered stats) (:stale stats)))
            (empty? @(:handlers server)) (empty? @(:supervisors server))
            (zero? (:live-packets stats)) (zero? (:connections stats))))))

(defn -main [mode-name destination]
  (when-not (and (= "true" (System/getenv "GITHUB_ACTIONS"))
                 (= "Linux" (System/getenv "RUNNER_OS")))
    (throw (ex-info "Performance measurements run on Linux GitHub runners only" {})))
  (let [mode (keyword mode-name)
        _ (when-not (contains? #{:off :timings :allocations :cpu :idle} mode)
            (throw (ex-info "Unknown profiling mode" {:mode mode})))
        server (bridge/start! #'handler
                              (if (contains? #{:timings :allocations} mode)
                                {:stage-profile {:mode mode :stride 8 :capacity 32768}} {}))
        observer (:profile server)
        calibration (atom nil)
        before (atom nil)
        native-before (atom nil)]
    (println "READY" (:port server)) (flush)
    (try
      (loop []
        (case (read-line)
          "calibrate"
          (do (quiesce! server)
              (when observer
                (reset! calibration (profiling/calibrate! observer (:pool server))))
              (println "CALIBRATED") (flush) (recur))
          "begin"
          (do (when @before (throw (ex-info "Profiling window already started" {})))
              (quiesce! server)
              (reset! native-before (bridge/stats server))
              (reset! before (profiling/counters))
              (when observer (profiling/recording! observer true))
              (when (= :cpu mode) (profile/sample! {:interval-ms 1 :depth 64}))
              (println "BEGUN") (flush) (recur))
          "end"
          (do (when-not @before (throw (ex-info "Profiling window has not started" {})))
              (quiesce! server)
              (when observer (profiling/recording! observer false))
              (when (= :cpu mode) (profile/stop-sampling!))
              (let [after (profiling/counters)
                    stats (bridge/stats server)
                    trace (when observer (profiling/capture observer))]
                (when trace (spit (str destination "-traces.edn") (pr-str trace)))
                (when (= :cpu mode)
                  (spit (str destination "-cpu.edn")
                        (pr-str {:counts (vec (profile/sample-counts))
                                 :lines (vec (profile/sample-lines))
                                 :stacks (vec (profile/samples))}))
                  (profile/write-folded (str destination "-cpu.folded")))
                (spit (str destination ".edn")
                      (pr-str {:mode mode :answered (- (:answered stats) (:answered @native-before))
                               :admitted (- (:admitted stats) (:admitted @native-before))
                               :stale (- (:stale stats) (:stale @native-before))
                               :counters (profiling/counter-delta @before after)
                               :stats stats :trace-info (when trace (dissoc trace :records))
                               :observer-calibration @calibration})))
              (println "ENDED") (flush) (recur))
          "stop" nil
          nil nil
          (throw (ex-info "Unknown profiling command" {}))))
      (finally
        (when (= :cpu mode) (profile/stop-sampling!))
        (prn {:stopped (bridge/stop! server)}) (flush)
        (shutdown-agents)))))
