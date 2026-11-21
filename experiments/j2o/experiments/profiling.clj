(ns j2o.experiments.profiling
  (:require [jolt.ffi :as ffi]
            [jolt.fibers :as fib]
            [jolt.profile :as profile]
            [jolt.scheme :as scheme]))

;; Only native-owned buffers cross this ABI. Scalar marks are bounded, allocate
;; nothing in C, and never enter Chez. Allocation/copy/destruction stays collect-safe.
(ffi/defcfn configure-native "j2o_probe_profile_configure" [:pointer :size_t :uint] :int :blocking)
(ffi/defcfn recording-native "j2o_probe_profile_recording" [:pointer :int] :int)
(ffi/defcfn packet-trace "j2o_probe_profile_trace" [:pointer] :pointer)
(ffi/defcfn trace-words "j2o_probe_profile_words" [] :size_t)
(ffi/defcfn trace-bytes "j2o_probe_profile_record_bytes" [] :size_t)
(ffi/defcfn stage-count "j2o_probe_profile_stages" [] :uint)
(ffi/defcfn stage-name "j2o_probe_profile_stage_name" [:uint] :string)
(ffi/defcfn scratch-reset-native "j2o_probe_profile_scratch_reset" [:pointer] :void)
(ffi/defcfn mark-native "j2o_probe_profile_mark" [:pointer :uint :uint64 :uint64] :void)
(ffi/defcfn trace-value "j2o_probe_profile_value" [:pointer :uint :uint] :uint64)
(ffi/defcfn info-native "j2o_probe_profile_info" [:pointer :uint] :uint64)
(ffi/defcfn copy-native "j2o_probe_profile_copy" [:pointer :size_t :pointer :size_t] :int :blocking)

;; Chez's occupied-byte count alone falls when GC reclaims objects. Adding
;; deallocated bytes yields cumulative allocation, as statistics does.
(def allocated-bytes (scheme/eval-string "(lambda () (+ (bytes-allocated) (bytes-deallocated)))"))
(def collections (scheme/eval-string "(lambda () (collections))"))

(defn create! [native {:keys [mode stride capacity]
                       :or {stride 8 capacity 32768}}]
  (when-not (contains? #{:timings :allocations} mode)
    (throw (ex-info "Unknown stage profiling mode" {:mode mode})))
  (when-not (zero? (configure-native native capacity stride))
    (throw (ex-info "Native profiling configuration rejected" {:capacity capacity :stride stride})))
  (let [stages (mapv #(keyword (stage-name %)) (range (stage-count)))]
    {:native native :mode mode :stages stages :indices (zipmap stages (range))
     :words (trace-words) :last-trace (volatile! 0)}))

(defn mark! [context trace stage]
  (when (pos? trace)
    (mark-native trace (get (:indices context) stage)
                 (if (= :allocations (:mode context)) (allocated-bytes) 0)
                 (if (= :allocations (:mode context)) (collections) 0))))

(defn decoding! [context packet]
  (let [trace (packet-trace packet)]
    (vreset! (:last-trace context) trace)
    (mark! context trace :decode-start)))

(defn recording! [context enabled?]
  (when-not (zero? (recording-native (:native context) (if enabled? 1 0)))
    (throw (ex-info "Native profiling recording state rejected" {}))))

(defn capture [context]
  (let [n (count (:stages context))
        info (into {} (map-indexed (fn [i k] [k (info-native (:native context) i)])
                                   [:considered :retained :dropped :stride :capacity]))]
    (ffi/with-alloc [out (* 8 (:words context))]
      (let [records
            (into []
                  (keep (fn [index]
                          (case (copy-native (:native context) index out (:words context))
                            0 nil
                            1 (let [data (vec (ffi/read-array out :uint64 (:words context)))]
                                {:id (nth data 0)
                                 :ns (subvec data 1 (+ 1 n))
                                 :allocated (subvec data (+ 1 n) (+ 1 (* 2 n)))
                                 :collections (subvec data (+ 1 (* 2 n)) (+ 1 (* 3 n)))
                                 :packet-native-bytes (nth data (+ 1 (* 3 n)))
                                 :response-native-bytes (nth data (+ 2 (* 3 n)))})
                            (throw (ex-info "Native profile copy rejected" {:index index})))))
                  (range (:retained info)))]
        (assoc info :stages (:stages context) :mode (:mode context)
               :completed (count records) :incomplete (- (:retained info) (count records))
               :records records)))))

(defn calibrate! [context pool]
  ;; Run the same observer twice on a carrier, with no application operation
  ;; between the marks. Export happens afterward and is outside that interval.
  (fib/join
    (fib/spawn
      (fn []
        (ffi/with-alloc [trace (trace-bytes)]
          (mapv (fn [_]
                  (scratch-reset-native trace)
                  (mark! context trace :decode-start)
                  (mark! context trace :request-decoded)
                  (let [a (get (:indices context) :decode-start)
                        b (get (:indices context) :request-decoded)]
                    {:ns (- (trace-value trace 0 b) (trace-value trace 0 a))
                     :allocated-bytes (- (trace-value trace 1 b) (trace-value trace 1 a))
                     :collections (- (trace-value trace 2 b) (trace-value trace 2 a))}))
                (range 256))))
      {:pool pool})))

(defn counters [] (profile/stats*))

(defn counter-delta [before after]
  (into {} (map (fn [k] [k (- (get after k) (get before k))])
                [:cpu-nanos :real-nanos :gc-count :gc-cpu-nanos :allocated-bytes])))
