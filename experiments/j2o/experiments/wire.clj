(ns j2o.experiments.wire
  (:require [clojure.string :as str]
            [jolt.socket]))

(defn connect [port]
  (let [socket (java.net.Socket. "127.0.0.1" port)]
    (.setSoTimeout socket 5000)
    {:socket socket :in (.getInputStream socket) :out (.getOutputStream socket)}))

(defn close! [client] (.close (:socket client)))

(defn send! [client value]
  (let [bytes (if (string? value) (.getBytes value "UTF-8") value)]
    (.write (:out client) bytes)
    (.flush (:out client))))

(defn line! [input]
  (let [buffer (StringBuilder.)]
    (loop [previous -1 count 0]
      (when (> count 65536) (throw (ex-info "Probe response line too long" {})))
      (let [value (.read input)]
        (cond
          (neg? value) (throw (ex-info "Unexpected response EOF" {}))
          (and (= previous 13) (= value 10)) (subs (.toString buffer) 0 (dec (.length buffer)))
          :else (do (.append buffer (char value)) (recur value (inc count))))))))

(defn bytes! [input n]
  (let [buffer (byte-array n)]
    (loop [offset 0]
      (when (< offset n)
        (let [read (.read input buffer offset (- n offset))]
          (when (<= read 0) (throw (ex-info "Truncated response body" {:offset offset :expected n})))
          (recur (+ offset read)))))
    buffer))

(defn response!
  ([client] (response! client false))
  ([client head?]
   (let [input (:in client)
         status-line (line! input)
         status (Long/parseLong (second (str/split status-line #" ")))
         headers (loop [result {}]
                   (let [line (line! input)]
                     (if (= line "") result
                       (let [colon (.indexOf line ":")]
                         (when (neg? colon) (throw (ex-info "Malformed response header" {:line line})))
                         (recur (assoc result (str/lower-case (subs line 0 colon))
                                              (str/trim (subs line (inc colon)))))))))
         body (cond
                (or head? (<= 100 status 199) (= status 204) (= status 304)) (byte-array 0)
                (contains? headers "content-length") (bytes! input (Long/parseLong (get headers "content-length")))
                (= "chunked" (get headers "transfer-encoding"))
                (let [out (java.io.ByteArrayOutputStream.)]
                  (loop []
                    (let [line (line! input)
                          n (Long/parseLong (first (str/split line #";")) 16)]
                      (if (zero? n)
                        (loop [] (when-not (= "" (line! input)) (recur)))
                        (do (.write out (bytes! input n))
                            (when-not (= "" (line! input)) (throw (ex-info "Invalid chunk suffix" {})))
                            (recur)))))
                  (.toByteArray out))
                :else (.readAllBytes input))]
     {:status status :headers headers :body body})))

(defn request!
  ([port path] (request! port "GET" path nil))
  ([port method path body]
   (let [client (connect port)
         bytes (cond (nil? body) (byte-array 0)
                     (string? body) (.getBytes body "UTF-8")
                     :else body)]
     (try
       (send! client (str method " " path " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
                          (when (pos? (alength bytes)) (str "Content-Length: " (alength bytes) "\r\n"))
                          "\r\n"))
       (when (pos? (alength bytes)) (send! client bytes))
       (response! client (= method "HEAD"))
       (finally (close! client))))))

(defn text [response] (String. (:body response) "UTF-8"))

(defn raw! [port message]
  (let [client (connect port)]
    (try (send! client message) (response! client)
         (finally (close! client)))))
