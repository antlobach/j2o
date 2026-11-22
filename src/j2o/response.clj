(ns j2o.response
  "Owned native response construction for unary Ring bodies."
  (:require [clojure.string :as str]
            [j2o.native :as native]))

(defn- body-bytes [body]
  (cond
    (nil? body) (byte-array 0)
    (string? body) (.getBytes body "UTF-8")
    (bytes? body) body
    :else (throw (ex-info "Unsupported unary response body" {:body body}))))

(defn- header-safe? [value]
  (not (or (str/includes? value "\r") (str/includes? value "\n"))))

(defn create! [id response]
  (when-not (map? response)
    (throw (ex-info "Handler did not return a response map" {:response response})))
  (let [status (get response :status 200)]
    (when-not (and (integer? status) (<= 200 status 599))
      (throw (ex-info "Invalid response status" {:status status})))
    (let [command (native/response-create! id status (body-bytes (:body response)))
          complete (volatile! false)]
      (try
        (doseq [[key value] (:headers response)]
          (let [header (if (keyword? key) (name key) (str key))]
            (when (header-safe? header)
              (doseq [item (if (vector? value) value [value])
                      :let [text (str item)]
                      :when (header-safe? text)]
                (native/response-header! command header text)))))
        (vreset! complete true)
        command
        (finally
          (when-not @complete (native/response-release! command)))))))
