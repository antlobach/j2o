(ns j2o.native
  "Internal, exact-version bridge ABI. Every variable-sized/locking call is collect-safe."
  (:require [clojure.string :as str]
            [jolt.ffi :as ffi]))

(def abi-version 0x80000002)
(def library-path
  (or (System/getenv "J2O_BRIDGE_LIBRARY")
      (str (System/getProperty "user.dir") "/.cache/native/libj2o_bridge."
           (if (str/includes? (System/getProperty "os.name") "Mac") "dylib" "so"))))

(ffi/load-library library-path)
(ffi/defcfn version* "j2o_abi_version" [] :uint32)
(let [actual (version*)]
  (when-not (= abi-version actual)
    (throw (ex-info "j2o native ABI mismatch; rebuild the bridge before starting a server"
                    {:expected abi-version :given actual :library library-path}))))

(def error-layout
  (ffi/layout [:struct [[:abi-size :uint32] [:result :int32] [:os-errno :int32]
                        [:message [:array :uint8 256]] [:syscall [:array :uint8 32]]
                        [:errno-name [:array :uint8 32]] [:key [:array :uint8 64]]
                        [:given [:array :uint8 128]]]]))
(def error-size (ffi/sizeof error-layout))
(when-not (= 524 error-size)
  (throw (ex-info "j2o error layout is incompatible" {:expected 524 :given error-size})))

(ffi/defcfn config-create* "j2o_config_create" [:pointer] :pointer :blocking)
(ffi/defcfn config-u64* "j2o_config_set_u64" [:pointer :uint32 :uint64 :pointer] :int :blocking)
(ffi/defcfn config-bool* "j2o_config_set_bool" [:pointer :uint32 :int :pointer] :int :blocking)
(ffi/defcfn config-string* "j2o_config_set_string" [:pointer :uint32 :pointer :size_t :pointer] :int :blocking)
(ffi/defcfn config-destroy! "j2o_config_destroy" [:pointer] :void :blocking)
(ffi/defcfn server-start* "j2o_server_start" [:pointer :pointer :pointer] :int :blocking)
(ffi/defcfn event-fd "j2o_server_event_fd" [:pointer] :int)
(ffi/defcfn server-port "j2o_server_port" [:pointer] :uint32)
(ffi/defcfn server-stat "j2o_server_stat" [:pointer :uint32] :uint64)
(ffi/defcfn event-take* "j2o_event_take" [:pointer :pointer :pointer] :int :blocking)
(ffi/defcfn event-kind "j2o_event_kind" [:pointer] :uint32)
(ffi/defcfn event-id* "j2o_event_id" [:pointer :pointer] :void)
(ffi/defcfn event-version "j2o_event_version" [:pointer] :int)
(ffi/defcfn event-headers "j2o_event_headers" [:pointer] :size_t)
(ffi/defcfn event-field* "j2o_event_field" [:pointer :uint32 :size_t :pointer] :pointer)
(ffi/defcfn event-release! "j2o_event_release" [:pointer] :void :blocking)
(ffi/defcfn response-create* "j2o_response_create" [:pointer :int :pointer :size_t :pointer] :pointer :blocking)
(ffi/defcfn response-header* "j2o_response_header" [:pointer :pointer :size_t :pointer :size_t :pointer] :int :blocking)
(ffi/defcfn response-submit* "j2o_response_submit" [:pointer :pointer :pointer] :int :blocking)
(ffi/defcfn response-release! "j2o_response_release" [:pointer] :void :blocking)
(ffi/defcfn begin-drain* "j2o_server_begin_drain" [:pointer :pointer] :int :blocking)
(ffi/defcfn force-stop* "j2o_server_force_stop" [:pointer :pointer] :int :blocking)
(ffi/defcfn join* "j2o_server_join" [:pointer :uint64 :pointer] :int :blocking)
(ffi/defcfn server-destroy! "j2o_server_destroy" [:pointer] :void :blocking)

(defn error-text [pointer offset capacity]
  ;; Bound the read even if a failed native library forgot its NUL terminator.
  (let [text (String. (ffi/read-array (+ pointer offset) capacity) "UTF-8")
        end (.indexOf text "\u0000")]
    (if (neg? end) text (subs text 0 end))))

(defn decode-error [pointer]
  (let [size (ffi/read pointer :uint32)]
    (when-not (= error-size size)
      (throw (ex-info "j2o returned an incompatible error record" {:expected error-size :given size})))
    {:result (ffi/read pointer :int32 4)
     :os-errno (ffi/read pointer :int32 8)
     :message (error-text pointer 12 256)
     :syscall (error-text pointer 268 32)
     :errno-name (error-text pointer 300 32)
     :key (error-text pointer 332 64)
     :given (error-text pointer 396 128)}))

(defn check! [result error]
  (when-not (zero? result)
    (let [data (decode-error error)]
      (throw (ex-info (:message data) data))))
  result)

(defn config-create! []
  (ffi/with-alloc [error error-size]
    (let [config (config-create* error)]
      (when (ffi/null? config)
        (let [data (decode-error error)] (throw (ex-info (:message data) data))))
      config)))

(defn config-u64! [config key value]
  (ffi/with-alloc [error error-size]
    (check! (config-u64* config key value error) error)))

(defn config-bool! [config key value]
  (ffi/with-alloc [error error-size]
    (check! (config-bool* config key (if value 1 0) error) error)))

(defn config-string! [config key value]
  (let [bytes (.getBytes value "UTF-8")]
    (ffi/with-alloc [error (+ error-size (max 1 (alength bytes)))]
      (let [text (+ error error-size)]
        (ffi/write-array text bytes)
        (check! (config-string* config key text (alength bytes) error) error)))))

(defn server-start! [config]
  (ffi/with-out [server :pointer]
    (ffi/with-alloc [error error-size]
      (check! (server-start* config server error) error)
      (ffi/read server :pointer))))

(defn event-take! [server]
  (ffi/with-out [event :pointer]
    (ffi/with-alloc [error error-size]
      (check! (event-take* server event error) error)
      (let [result (ffi/read event :pointer)]
        (when-not (ffi/null? result) result)))))

(defn event-id [event]
  (ffi/with-alloc [id 16]
    (event-id* event id)
    (ffi/read-array id 16)))

(defn event-field [event field index bytes?]
  (ffi/with-out [size :size_t]
    (let [data (event-field* event field index size)
          n (ffi/read size :size_t)]
      (if bytes? (ffi/read-array data n) (ffi/read-bytes data n)))))

(defn response-create! [id status body]
  (let [length (alength body)]
    (ffi/with-alloc [memory (+ 16 error-size (max 1 length))]
      (let [error (+ memory 16)
            data (+ error error-size)]
        (ffi/write-array memory id)
        (ffi/write-array data body)
        (let [response (response-create* memory status data length error)]
          (when (ffi/null? response)
            (let [failure (decode-error error)]
              (throw (ex-info (:message failure) failure))))
          response)))))

(defn response-header! [response name value]
  (let [name-bytes (.getBytes name "UTF-8")
        value-bytes (.getBytes value "UTF-8")
        name-size (alength name-bytes)
        value-size (alength value-bytes)]
    (ffi/with-alloc [memory (+ error-size (max 1 name-size) (max 1 value-size))]
      (let [name-ptr (+ memory error-size)
            value-ptr (+ name-ptr (max 1 name-size))]
        (ffi/write-array name-ptr name-bytes)
        (ffi/write-array value-ptr value-bytes)
        (check! (response-header* response name-ptr name-size value-ptr value-size memory) memory)))))

(defn response-submit! [server response]
  (ffi/with-alloc [error error-size]
    (check! (response-submit* server response error) error)))

(defn begin-drain! [server]
  (ffi/with-alloc [error error-size]
    (check! (begin-drain* server error) error)))

(defn force-stop! [server]
  (ffi/with-alloc [error error-size]
    (check! (force-stop* server error) error)))

(defn join! [server timeout-ms]
  (ffi/with-alloc [error error-size]
    (check! (join* server timeout-ms error) error)))
