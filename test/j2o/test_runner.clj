(ns j2o.test-runner
  (:require [j2o.native :as native]
            [j2o.ring-smoke :as ring-smoke]
            [jolt.ffi :as ffi]))

(defn equal! [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str "Failed: " label) {:expected expected :given actual}))))

(defn error-layout! []
  (equal! :error-size 524 native/error-size)
  (doseq [[field offset] [[:abi-size 0] [:result 4] [:os-errno 8] [:message 12]
                         [:syscall 268] [:errno-name 300] [:key 332] [:given 396]]]
    (equal! field offset (ffi/field-offset native/error-layout field))))

(defn native-smoke! []
  (let [config (native/config-create!)]
    (try
      (let [failure (try (native/config-u64! config 2 65536) nil
                         (catch clojure.lang.ExceptionInfo error (ex-data error)))]
        (equal! :invalid-port-result 1 (:result failure))
        (equal! :native-error-has-copied-value "65536" (:given failure))
        (native/config-u64! config 2 0)
        (native/config-string! config 1 "localhost")
        (let [server (native/server-start! config)]
          (try
            (let [failure (try (native/join! server 0) nil
                               (catch clojure.lang.ExceptionInfo error (ex-data error)))]
              (equal! :live-server-join-retains-state "ETIMEDOUT" (:errno-name failure)))
            (native/begin-drain! server)
            (native/force-stop! server)
            (native/join! server 5000)
            (native/join! server 0)
            (equal! :borrowed-event-fd-retained-until-destroy true (>= (native/event-fd server) 0))
            (equal! :earlier-error-survives-further-native-calls "65536" (:given failure))
            (finally
              (native/force-stop! server)
              (native/join! server 5000)
              (native/server-destroy! server)))))
      (finally (native/config-destroy! config)))))

(defn -main [& _]
  (try
    (error-layout!)
    (dotimes [_ 20] (native-smoke!))
    (ring-smoke/run!)
    (println "all passed: managed ABI, copied error lifetime and native lifecycle smoke")
    (finally (shutdown-agents))))
