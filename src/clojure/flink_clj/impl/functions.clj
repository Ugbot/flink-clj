(ns flink-clj.impl.functions
  "Internal: Creates Flink function wrappers from Clojure functions.

  This namespace handles the conversion of Clojure vars/functions into
  serializable Flink function objects. Supports both Flink 1.20 and 2.x
  through version-aware class loading.")

;; Detect Flink version at load time
(def ^:private flink-version
  "Detect Flink version by checking RuntimeEnvironment version string."
  (try
    (let [version-class (Class/forName "org.apache.flink.runtime.util.EnvironmentInformation")
          get-version (.getMethod version-class "getVersion" (into-array Class []))
          version-str (str (.invoke get-version nil (into-array Object [])))]
      (if (.startsWith version-str "2.")
        :v2
        :v1.20))
    (catch Exception _
      ;; Fallback: check if v120 classes exist (they're compiled with flink-1.20 profile)
      (try
        (Class/forName "flink_clj.v120.functions.CljMapFunction")
        :v1.20
        (catch ClassNotFoundException _
          :v2)))))

(def ^:private package-prefix
  "Package prefix for version-specific function wrappers."
  (case flink-version
    :v2 "flink_clj.v2.functions."
    :v1.20 "flink_clj.v120.functions."))

(defn- load-wrapper-class
  "Load a wrapper class by name from the appropriate version package."
  [class-name]
  (Class/forName (str package-prefix class-name)))

(defn- make-instance
  "Create an instance of a wrapper class with string arguments."
  ([class-name arg1]
   (let [clazz (load-wrapper-class class-name)
         ctor (.getConstructor clazz (into-array Class [String]))]
     (.newInstance ctor (into-array Object [arg1]))))
  ([class-name arg1 arg2]
   (let [clazz (load-wrapper-class class-name)
         ctor (.getConstructor clazz (into-array Class [String String]))]
     (.newInstance ctor (into-array Object [arg1 arg2])))))

(defn var->ns-name
  "Extract namespace string and function name from a var.

  Returns [namespace-string function-name-string].

  Throws if the input is not a var."
  [v]
  (if (var? v)
    [(str (.-ns v)) (str (.-sym v))]
    (throw (ex-info "Function must be a var (use defn, not fn or #())"
                    {:value v
                     :type (type v)
                     :hint "Define your function at the top level with defn"}))))

(defn fn->var
  "Convert a function reference to a var if possible.

  Handles:
  - Vars: returned as-is
  - Keywords: handled specially for key selectors
  - Other: throws with helpful error"
  [f]
  (cond
    (var? f) f
    (keyword? f) f ; Keywords handled separately
    (fn? f) (throw (ex-info
                     "Anonymous functions cannot be serialized by Flink"
                     {:hint "Use defn to define the function at top level"
                      :example "(defn my-fn [x] (* x 2))\n(map stream my-fn)"}))
    :else (throw (ex-info
                   "Expected a var or keyword"
                   {:value f
                    :type (type f)}))))

(defn make-map-function
  "Create a CljMapFunction from a Clojure var."
  [f]
  (let [[ns-str fn-name] (var->ns-name (fn->var f))]
    (make-instance "CljMapFunction" ns-str fn-name)))

(defn make-filter-function
  "Create a CljFilterFunction from a Clojure predicate var."
  [pred]
  (let [[ns-str fn-name] (var->ns-name (fn->var pred))]
    (make-instance "CljFilterFunction" ns-str fn-name)))

(defn make-flat-map-function
  "Create a CljFlatMapFunction from a Clojure var."
  [f]
  (let [[ns-str fn-name] (var->ns-name (fn->var f))]
    (make-instance "CljFlatMapFunction" ns-str fn-name)))

(defn make-reduce-function
  "Create a CljReduceFunction from a Clojure var."
  [f]
  (let [[ns-str fn-name] (var->ns-name (fn->var f))]
    (make-instance "CljReduceFunction" ns-str fn-name)))

(defn make-key-selector
  "Create a CljKeySelector from a Clojure var or keyword.

  Keywords are handled specially - they act as functions on maps."
  [f]
  (cond
    (keyword? f)
    (make-instance "CljKeySelector" (name f))

    (var? f)
    (let [[ns-str fn-name] (var->ns-name f)]
      (make-instance "CljKeySelector" ns-str fn-name))

    :else
    (throw (ex-info "Key selector must be a keyword or var"
                    {:value f
                     :type (type f)
                     :example "(key-by stream :user-id) or (key-by stream my-key-fn)"}))))

(defn- handler->ns-name
  "Extract namespace and name from a handler function, returning [ns name] or [nil nil]."
  [handler]
  (if (and handler (var? handler))
    (var->ns-name handler)
    [nil nil]))

(defn make-keyed-process-function
  "Create a CljKeyedProcessFunction from a handlers map.

  Handlers map keys: :process, :on-timer, :open, :close"
  [handlers state-descriptors]
  (let [{:keys [process on-timer open close]} handlers
        [process-ns process-name] (handler->ns-name process)
        [on-timer-ns on-timer-name] (handler->ns-name on-timer)
        [open-ns open-name] (handler->ns-name open)
        [close-ns close-name] (handler->ns-name close)
        clazz (load-wrapper-class "CljKeyedProcessFunction")
        ctor (.getConstructor clazz (into-array Class (repeat 8 String)))]
    (.newInstance ctor (into-array Object
                                   [process-ns process-name
                                    on-timer-ns on-timer-name
                                    open-ns open-name
                                    close-ns close-name]))))

(defn make-process-function
  "Create a CljProcessFunction from a handlers map.

  Handlers map keys: :process, :open, :close"
  [handlers]
  (let [{:keys [process open close]} handlers
        [process-ns process-name] (handler->ns-name process)
        [open-ns open-name] (handler->ns-name open)
        [close-ns close-name] (handler->ns-name close)
        clazz (load-wrapper-class "CljProcessFunction")
        ctor (.getConstructor clazz (into-array Class (repeat 6 String)))]
    (.newInstance ctor (into-array Object
                                   [process-ns process-name
                                    open-ns open-name
                                    close-ns close-name]))))

(defn make-aggregate-function
  "Create a CljAggregateFunction from an aggregator spec map.

  Spec map keys:
    :create-accumulator - fn [] -> acc (required)
    :add - fn [acc element] -> acc (required)
    :get-result - fn [acc] -> result (required)
    :merge - fn [acc1 acc2] -> acc (optional, needed for session windows)

  output-type is optional TypeInformation for the result."
  [agg-spec output-type]
  (let [{:keys [create-accumulator add get-result merge]} agg-spec
        [create-ns create-name] (handler->ns-name create-accumulator)
        [add-ns add-name] (handler->ns-name add)
        [result-ns result-name] (handler->ns-name get-result)
        [merge-ns merge-name] (handler->ns-name merge)
        clazz (load-wrapper-class "CljAggregateFunction")
        ;; Constructor takes 8 strings + TypeInformation
        ctor (.getConstructor clazz
               (into-array Class [String String String String
                                  String String String String
                                  org.apache.flink.api.common.typeinfo.TypeInformation]))]
    (.newInstance ctor (into-array Object
                                   [create-ns create-name
                                    add-ns add-name
                                    result-ns result-name
                                    merge-ns merge-name
                                    output-type]))))

(defn make-co-map-function
  "Create a CljCoMapFunction from two Clojure vars.

  map1-fn handles elements from first stream.
  map2-fn handles elements from second stream.
  output-type is optional TypeInformation for the result."
  [map1-fn map2-fn output-type]
  (let [[map1-ns map1-name] (var->ns-name (fn->var map1-fn))
        [map2-ns map2-name] (var->ns-name (fn->var map2-fn))
        clazz (load-wrapper-class "CljCoMapFunction")
        ctor (.getConstructor clazz
               (into-array Class [String String String String
                                  org.apache.flink.api.common.typeinfo.TypeInformation]))]
    (.newInstance ctor (into-array Object
                                   [map1-ns map1-name
                                    map2-ns map2-name
                                    output-type]))))

(defn make-co-flat-map-function
  "Create a CljCoFlatMapFunction from two Clojure vars.

  flat-map1-fn handles elements from first stream (returns seq).
  flat-map2-fn handles elements from second stream (returns seq).
  output-type is optional TypeInformation for the result."
  [flat-map1-fn flat-map2-fn output-type]
  (let [[fm1-ns fm1-name] (var->ns-name (fn->var flat-map1-fn))
        [fm2-ns fm2-name] (var->ns-name (fn->var flat-map2-fn))
        clazz (load-wrapper-class "CljCoFlatMapFunction")
        ctor (.getConstructor clazz
               (into-array Class [String String String String
                                  org.apache.flink.api.common.typeinfo.TypeInformation]))]
    (.newInstance ctor (into-array Object
                                   [fm1-ns fm1-name
                                    fm2-ns fm2-name
                                    output-type]))))

(defn make-join-function
  "Create a CljJoinFunction from a Clojure var.

  join-fn receives two elements (one from each stream) and returns a joined result."
  [join-fn]
  (let [[ns-str fn-name] (var->ns-name (fn->var join-fn))]
    (make-instance "CljJoinFunction" ns-str fn-name)))

(defn make-flat-join-function
  "Create a CljFlatJoinFunction from a Clojure var.

  join-fn receives two elements and returns a sequence of joined results."
  [join-fn]
  (let [[ns-str fn-name] (var->ns-name (fn->var join-fn))]
    (make-instance "CljFlatJoinFunction" ns-str fn-name)))

(defn make-process-join-function
  "Create a CljProcessJoinFunction from a Clojure var.

  join-fn receives two elements from an interval join.
  flat-output? if true, treats result as sequence and emits each element."
  [join-fn flat-output?]
  (let [[ns-str fn-name] (var->ns-name (fn->var join-fn))
        clazz (load-wrapper-class "CljProcessJoinFunction")
        ctor (.getConstructor clazz (into-array Class [String String Boolean/TYPE]))]
    (.newInstance ctor (into-array Object [ns-str fn-name (boolean flat-output?)]))))

(defn make-process-window-function
  "Create a CljProcessWindowFunction from a Clojure var.

  process-fn receives a context map with :key, :window, and :elements.
  flat-output? if true, treats result as sequence and emits each element."
  [process-fn flat-output?]
  (let [[ns-str fn-name] (var->ns-name (fn->var process-fn))
        clazz (load-wrapper-class "CljProcessWindowFunction")
        ctor (.getConstructor clazz (into-array Class [String String Boolean/TYPE]))]
    (.newInstance ctor (into-array Object [ns-str fn-name (boolean flat-output?)]))))

(defn make-async-function
  "Create a CljAsyncFunction from a Clojure var.

  async-fn receives an input and returns a value, collection, or CompletableFuture.
  flat-output? if true, treats collection results as multiple outputs."
  [async-fn flat-output?]
  (let [[ns-str fn-name] (var->ns-name (fn->var async-fn))
        clazz (load-wrapper-class "CljAsyncFunction")
        ctor (.getConstructor clazz (into-array Class [String String Boolean/TYPE]))]
    (.newInstance ctor (into-array Object [ns-str fn-name (boolean flat-output?)]))))

(defn make-broadcast-process-function
  "Create a CljBroadcastProcessFunction from two Clojure vars.

  process-fn handles non-broadcast stream elements (read-only state access).
  broadcast-fn handles broadcast stream elements (can modify state).
  flat-output? if true, treats collection results as multiple outputs."
  [process-fn broadcast-fn flat-output?]
  (let [[process-ns process-name] (if process-fn (handler->ns-name process-fn) [nil nil])
        [broadcast-ns broadcast-name] (if broadcast-fn (handler->ns-name broadcast-fn) [nil nil])
        clazz (load-wrapper-class "CljBroadcastProcessFunction")
        ctor (.getConstructor clazz (into-array Class [String String String String Boolean/TYPE]))]
    (.newInstance ctor (into-array Object
                                   [process-ns process-name
                                    broadcast-ns broadcast-name
                                    (boolean flat-output?)]))))

(defn make-session-gap-extractor
  "Create a SessionWindowTimeGapExtractor from a Clojure var.

  gap-fn receives an element and returns the gap duration in milliseconds."
  [gap-fn]
  (let [[ns-str fn-name] (var->ns-name (fn->var gap-fn))
        ;; Load from common package - not version specific
        clazz (Class/forName "flink_clj.CljSessionGapExtractor")
        ctor (.getConstructor clazz (into-array Class [String String]))]
    (.newInstance ctor (into-array Object [ns-str fn-name]))))

(defn make-cogroup-function
  "Create a CljCoGroupFunction from a Clojure var.

  cogroup-fn receives {:left [...] :right [...]} and returns result(s).
  flat-output? if true, treats collection results as multiple outputs."
  [cogroup-fn flat-output?]
  (let [[ns-str fn-name] (var->ns-name (fn->var cogroup-fn))
        ;; Load from common package - not version specific
        clazz (Class/forName "flink_clj.CljCoGroupFunction")
        ctor (.getConstructor clazz (into-array Class [String String Boolean/TYPE]))]
    (.newInstance ctor (into-array Object [ns-str fn-name (boolean flat-output?)]))))

;; =============================================================================
;; Public Helpers for Watermark and Other Custom Generators
;; =============================================================================

(defn fn->ns-name
  "Extract namespace and function name from a var.

  Wrapper around var->ns-name that accepts vars directly.
  Returns [namespace-string function-name-string].

  Throws if input is not a var (use defn to define functions)."
  [f]
  (var->ns-name (fn->var f)))

(defn resolve-fn
  "Resolve a Clojure function at runtime from namespace and name strings.

  Used by watermark generators and other components that need to
  load functions dynamically after deserialization.

  Ensures the namespace is loaded before resolving the var."
  [ns-str fn-name]
  (require (symbol ns-str))
  (resolve (symbol ns-str fn-name)))
