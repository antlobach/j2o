(ns j2o.compat-manifest
  "Run from the pinned reference checkout, never from j2o's classpath."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def ^:dynamic *mismatches* nil)

(defn equal! [label expected actual]
  (when-not (= expected actual)
    (let [failure {:contract label :expected expected :actual actual}]
      (if *mismatches*
        (do (prn {:mismatch failure}) (swap! *mismatches* conj failure))
        (throw (ex-info (str "Reference contract mismatch: " label) failure))))))

(defn source-forms [file]
  (with-open [reader (java.io.PushbackReader. (io/reader file))]
    (loop [forms []]
      (let [form (read {:eof ::eof} reader)]
        (if (= ::eof form) forms (recur (conj forms form)))))))

(defn signature [arglists]
  (reduce (fn [result args]
            (if (some #{'&} args)
              (assoc result :variadic-min (count (take-while #(not= '& %) args)))
              (update result :arities (fnil conj []) (count args))))
          {} arglists))

(defn normalize-signature [value]
  (cond-> (select-keys value [:arities :variadic-min])
    (:arities value) (update :arities #(vec (sort %)))))

(defn declarations [forms]
  (into {}
        (keep (fn [form]
                (when (and (seq? form) (symbol? (first form)))
                  (let [head (name (first form)), identifier (second form)]
                    (cond
                      (= "defcfn" head)
                      [identifier {:kind :ffi :arities [(count (first (filter vector? (drop 2 form))))]}]
                      (#{"defn" "defn-"} head)
                      (let [body (drop-while #(or (string? %) (map? %)) (drop 2 form))
                            args (if (vector? (first body)) [(first body)] (map first body))]
                        [identifier (signature args)])
                      :else nil))))
              forms)))

(defn reference-signature [identifier seen]
  (when (contains? seen identifier)
    (throw (ex-info "Cyclic reference alias" {:var identifier :seen seen})))
  (let [module (symbol (namespace identifier))
        variable (ns-resolve module (symbol (name identifier)))
        path (str (-> (str module) (str/replace "." "/") (str/replace "-" "_")) ".clj")
        resource (io/resource path)]
    (when-not resource (throw (ex-info "Reference source unavailable" {:var identifier :path path})))
    (let [forms (source-forms resource)
          direct (get (declarations forms) (symbol (name identifier)))]
      (or direct
          (let [form (first (filter #(and (seq? %) (= 'def (first %))
                                         (= (symbol (name identifier)) (second %))) forms))
                alias (when (symbol? (nth form 2 nil)) (ns-resolve module (nth form 2)))
                metadata (when alias (meta alias))]
            (when-not alias (throw (ex-info "No source arity for reference var" {:var identifier :metadata (meta variable)})))
            (reference-signature (symbol (str (ns-name (:ns metadata))) (str (:name metadata)))
                                 (conj seen identifier)))))))

(defn check-reference! [root]
  (binding [*mismatches* (atom [])]
  (let [manifest (edn/read-string (slurp (str root "/test/compat/public-api.edn")))
        expected (:vars manifest)
        files (filter #(and (.isFile %) (str/ends-with? (.getName %) ".clj"))
                      (file-seq (io/file "src/ring_chez")))
        modules (into {} (map (fn [file]
                               (let [forms (source-forms file)]
                                 [(second (first (filter #(and (seq? %) (= 'ns (first %))) forms))) forms])) files))]
    (when (empty? modules) (throw (ex-info "Run from the reference checkout" {})))
    (doseq [module (keys modules)] (require module))
    (let [actual (into {} (for [module (keys modules), [identifier variable] (ns-publics module)]
                            [(symbol (str module) (str identifier)) variable]))]
      (equal! :complete-public-var-set (set (keys expected)) (set (keys actual)))
      (doseq [[identifier contract] expected]
        (when-not (#{:supported :internal-excluded} (:status contract))
          (throw (ex-info "Missing compatibility decision" {:var identifier :contract contract})))
        (when (and (= :internal-excluded (:status contract)) (str/blank? (:reason contract)))
          (throw (ex-info "Excluded var lacks a reason" {:var identifier})))
        (let [variable (get actual identifier)
              module (symbol (namespace identifier))
              source (get (declarations (get modules module)) (symbol (name identifier)))]
          (case (:kind contract)
            :constant
            (equal! identifier (:value contract)
                    (if (= :byte-array (:representation contract)) (vec @variable) @variable))
            :alias
            (let [target (resolve (:target contract))]
              (when-not target (throw (ex-info "Alias target not found" {:var identifier :target (:target contract)})))
              (equal! [identifier :identity] true (identical? @target @variable))
              (equal! [identifier :arities] (normalize-signature contract)
                      (normalize-signature (reference-signature (:target contract) #{}))))
            :ffi
            (do (equal! [identifier :kind] :ffi (:kind source))
                (equal! [identifier :arities] (normalize-signature contract) (normalize-signature source)))
            :function
            (let [record (first (filter #(and (seq? %) (= 'defrecord (first %))
                                              (#{(str "->" (second %)) (str "map->" (second %))} (name identifier)))
                                       (get modules module)))
                  derived (if record
                            {:arities [(if (str/starts-with? (name identifier) "map->") 1 (count (nth record 2)))]}
                            (or source (signature (:arglists (meta variable)))))]
              (equal! [identifier :arities] (normalize-signature contract) (normalize-signature derived)))
            (throw (ex-info "Unknown manifest kind" {:var identifier :contract contract}))))))
    (let [types (set (for [[module forms] modules, form forms
                          :when (and (seq? form) (= 'defrecord (first form)))]
                      (symbol (str module "." (second form)))))]
      (equal! :record-type-set (set (keys (:types manifest))) types)
      (doseq [[identifier contract] (:types manifest)]
        (equal! [identifier :status] :supported (:status contract))
        (equal! [identifier :representation] :opaque (:representation contract))
        (when (str/blank? (:reason contract))
          (throw (ex-info "Opaque type decision lacks a reason" {:type identifier})))))
    (equal! :handle-contract-keys #{:socket :sockets} (set (keys (:handle-shape manifest))))
    (let [socket (get-in manifest [:handle-shape :socket])
          sockets (get-in manifest [:handle-shape :sockets])]
      (equal! :socket-status :supported (:status socket))
      (equal! :socket-representation :opaque-listener-token (:representation socket))
      (equal! :sockets-status :internal-excluded (:status sockets))
      (when (or (str/blank? (:excluded-meaning socket)) (str/blank? (:reason sockets)))
        (throw (ex-info "Handle contract lacks its exclusion rationale" {}))))
    (let [reference (edn/read-string (slurp "deps.edn"))
          project (edn/read-string (slurp (str root "/deps.edn")))]
      (equal! :dependency-contract (:deps reference) (:deps project))
      (equal! :optional-native-contract (:jolt/native reference) (:jolt/native project)))
    (when (seq @*mismatches*)
      (throw (ex-info "Reference compatibility manifest mismatches" {:mismatches @*mismatches*})))
    (prn {:scenario :pinned-reference-public-api :vars (count expected)
          :decisions (frequencies (map :status (vals expected))) :status :passed}))))
