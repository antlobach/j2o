(ns j2o.request
  "Conversion of an owned native request event to a Ring request."
  (:require [clojure.string :as str]
            [j2o.native :as native]))

;; URI policy matches ring-chez's normalize-path (EPL-2.0), pinned in the
;; compatibility manifest; normalize before middleware sees the path.
(defn- normalize-path [path]
  (let [segments (reduce (fn [result segment]
                           (cond
                             (or (= segment "") (= segment ".")) result
                             (= segment "..") (if (seq result) (pop result) result)
                             :else (conj result segment)))
                         [] (str/split path #"/" -1))]
    (if (empty? segments) "/" (str "/" (str/join "/" segments)))))

(defn- host-name [host]
  (let [host (str/trim host)
        colon (str/last-index-of host ":")]
    (if (and colon (pos? colon) (< (inc colon) (count host))
             (every? #(<= (int \0) (int %) (int \9)) (subs host (inc colon))))
      (subs host 0 colon)
      host)))

(defn from-event [event port bind-host]
  (let [field (fn [number] (native/event-field event number 0 false))
        target (field 2)
        query (.indexOf target "?")
        raw-uri (if (neg? query) target (subs target 0 query))
        authority (field 4)
        headers (reduce (fn [result index]
                          (let [name (str/lower-case (native/event-field event 6 index false))
                                value (str/trim (native/event-field event 7 index false))]
                            (assoc result name (if-let [prior (get result name)]
                                                 (str prior "," value) value))))
                        {} (range (native/event-headers event)))
        headers (if (or (empty? authority) (contains? headers "host"))
                  headers (assoc headers "host" authority))
        body (native/event-field event 3 0 true)]
    {:server-port port
     :server-name (if-let [host (get headers "host")] (host-name host) bind-host)
     :remote-addr (field 5)
     :uri (normalize-path raw-uri)
     :ring-chez/raw-uri raw-uri
     :query-string (when-not (neg? query) (subs target (inc query)))
     :scheme :http
     :request-method (keyword (str/lower-case (field 1)))
     :protocol (case (native/event-version event)
                 256 "HTTP/1.0" 257 "HTTP/1.1" 512 "HTTP/2")
     :headers headers
     :body (when (pos? (alength body)) (java.io.ByteArrayInputStream. body))}))
