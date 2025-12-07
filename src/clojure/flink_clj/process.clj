(ns flink-clj.process
  "Process functions for advanced stream processing with state and timers.

  ProcessFunctions provide:
  - Access to element timestamps
  - Access to keyed state
  - Timer registration (event time and processing time)
  - Side outputs

  Types of process functions:
  - ProcessFunction: Basic processing without key access
  - KeyedProcessFunction: Processing with keyed state and timers

  Example:
    (require '[flink-clj.process :as p]
             '[flink-clj.state :as state])

    ;; Define state descriptors
    (def count-desc (state/value-state \"count\" :long))

    ;; Define process function handlers
    (defn my-process-element
      \"Process each element with state access.\"
      [ctx element out]
      (let [count-state (.getState (.getRuntimeContext ctx) count-desc)
            current (or (state/value count-state) 0)
            new-count (inc current)]
        (state/update! count-state new-count)
        (.collect out [element new-count])))

    ;; Apply to stream
    (-> keyed-stream
        (p/keyed-process {:process my-process-element}))"
  (:require [flink-clj.impl.functions :as impl]
            [flink-clj.types :as types])
  (:import [org.apache.flink.streaming.api.datastream DataStream KeyedStream SingleOutputStreamOperator]
           [org.apache.flink.api.common.typeinfo TypeInformation]
           [org.apache.flink.util OutputTag]))

;; =============================================================================
;; Type Resolution Helper
;; =============================================================================

(defn- resolve-type-info
  "Resolve a type hint to TypeInformation."
  [type-hint]
  (if (instance? TypeInformation type-hint)
    type-hint
    (types/from-spec type-hint)))

;; =============================================================================
;; Output Tags for Side Outputs
;; =============================================================================

(defn output-tag
  "Create an OutputTag for side outputs.

  Example:
    (def late-data-tag (output-tag \"late-data\" :string))
    (def error-tag (output-tag \"errors\" [:row :message :string :element :clojure]))"
  [id type-spec]
  (OutputTag. ^String id ^TypeInformation (resolve-type-info type-spec)))

;; =============================================================================
;; KeyedProcessFunction
;; =============================================================================

(defn keyed-process
  "Apply a KeyedProcessFunction to a KeyedStream.

  The handlers map can contain:
    :process - (fn [ctx element out] ...) - Process each element
               ctx has: .timestamp, .timerService, .getCurrentKey, .getRuntimeContext
               out is a Collector to emit results
    :on-timer - (fn [ctx timestamp out] ...) - Handle timer firing
    :open - (fn [ctx] ...) - Initialize resources (called once)
    :close - (fn [] ...) - Cleanup resources (called once)

  Options:
    :returns - TypeInformation or spec for output type
    :state - Map of state-name -> state-descriptor for automatic registration

  Example:
    (defn process-element [ctx element out]
      (let [ts (.timestamp ctx)]
        (.collect out {:element element :timestamp ts})))

    (defn handle-timer [ctx timestamp out]
      (.collect out {:timer-fired timestamp}))

    (-> keyed-stream
        (keyed-process {:process process-element
                        :on-timer handle-timer}
                       {:returns [:row :element :clojure :timestamp :long]}))"
  ([^KeyedStream stream handlers]
   (keyed-process stream handlers nil))
  ([^KeyedStream stream handlers {:keys [returns state]}]
   (let [process-fn (impl/make-keyed-process-function handlers state)
         result (.process stream process-fn)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

;; =============================================================================
;; ProcessFunction (non-keyed)
;; =============================================================================

(defn process
  "Apply a ProcessFunction to a DataStream (non-keyed).

  The handlers map can contain:
    :process - (fn [ctx element out] ...) - Process each element
               ctx has: .timestamp, .timerService (limited), .output (side outputs)
               out is a Collector to emit results
    :open - (fn [ctx] ...) - Initialize resources
    :close - (fn [] ...) - Cleanup resources

  Options:
    :returns - TypeInformation or spec for output type

  Example:
    (defn filter-and-tag [ctx element out]
      (if (valid? element)
        (.collect out element)
        (.output ctx error-tag {:error \"invalid\" :element element})))

    (-> stream
        (process {:process filter-and-tag}))"
  ([^DataStream stream handlers]
   (process stream handlers nil))
  ([^DataStream stream handlers {:keys [returns]}]
   (let [process-fn (impl/make-process-function handlers)
         result (.process stream process-fn)]
     (if returns
       (.returns result ^TypeInformation (resolve-type-info returns))
       result))))

;; =============================================================================
;; Timer Utilities
;; =============================================================================

(defn register-event-time-timer!
  "Register an event time timer.

  Call this from within a process function's :process handler.

  Example:
    (defn my-process [ctx element out]
      (let [timer-service (.timerService ctx)
            fire-at (+ (.timestamp ctx) 10000)]
        (register-event-time-timer! timer-service fire-at)
        (.collect out element)))"
  [timer-service timestamp]
  (.registerEventTimeTimer timer-service timestamp))

(defn register-processing-time-timer!
  "Register a processing time timer.

  Example:
    (defn my-process [ctx element out]
      (let [timer-service (.timerService ctx)
            fire-at (+ (System/currentTimeMillis) 10000)]
        (register-processing-time-timer! timer-service fire-at)
        (.collect out element)))"
  [timer-service timestamp]
  (.registerProcessingTimeTimer timer-service timestamp))

(defn delete-event-time-timer!
  "Delete a previously registered event time timer."
  [timer-service timestamp]
  (.deleteEventTimeTimer timer-service timestamp))

(defn delete-processing-time-timer!
  "Delete a previously registered processing time timer."
  [timer-service timestamp]
  (.deleteProcessingTimeTimer timer-service timestamp))

(defn current-processing-time
  "Get the current processing time from the timer service."
  [timer-service]
  (.currentProcessingTime timer-service))

(defn current-watermark
  "Get the current watermark (event time progress) from the timer service."
  [timer-service]
  (.currentWatermark timer-service))

;; =============================================================================
;; Side Output Access
;; =============================================================================

(defn get-side-output
  "Get a side output stream from a processed stream.

  Example:
    (def late-tag (output-tag \"late\" :string))

    (def main-stream (-> stream (process {:process my-fn})))
    (def late-stream (get-side-output main-stream late-tag))"
  [^SingleOutputStreamOperator stream ^OutputTag tag]
  (.getSideOutput stream tag))

;; =============================================================================
;; Context Helpers
;; =============================================================================

(defn timestamp
  "Get the timestamp of the current element from process context."
  [ctx]
  (.timestamp ctx))

(defn timer-service
  "Get the timer service from process context."
  [ctx]
  (.timerService ctx))

(defn current-key
  "Get the current key from keyed process context."
  [ctx]
  (.getCurrentKey ctx))

(defn runtime-context
  "Get the runtime context for state access."
  [ctx]
  (.getRuntimeContext ctx))

;; =============================================================================
;; Async State Processing (Flink 2.x only)
;; =============================================================================
;;
;; Flink 2.x introduces async state access via the State V2 API.
;; This allows non-blocking state operations for improved throughput.
;;
;; To use async state:
;; 1. Enable async state on the KeyedStream:
;;    (keyed/enable-async-state keyed-stream)
;;
;; 2. In your process function, use async state operations:
;;    (state/async-value state) -> StateFuture
;;    (state/async-update! state value) -> StateFuture
;;
;; 3. Chain operations on StateFuture:
;;    (state/then-apply future fn)
;;    (state/then-compose future fn)
;;    (state/then-accept future consumer)
;;
;; Example:
;;   (require '[flink-clj.keyed :as k]
;;            '[flink-clj.state :as state])
;;
;;   (def count-desc (state/value-state "count" :long))
;;
;;   (defn async-count-process
;;     "Count elements per key using async state."
;;     [ctx element out]
;;     (let [count-state (.getState (.getRuntimeContext ctx) count-desc)]
;;       (-> (state/async-value count-state)
;;           (state/then-apply #(inc (or % 0)))
;;           (state/then-accept
;;             (fn [new-count]
;;               ;; Update state asynchronously
;;               (state/async-update! count-state new-count)
;;               ;; Emit result
;;               (.collect out {:element element :count new-count}))))))
;;
;;   (-> stream
;;       (k/key-by :user-id)
;;       (k/enable-async-state)  ; Enable async state (Flink 2.x only)
;;       (keyed-process {:process #'async-count-process}))
;;
;; IMPORTANT: When async state is enabled:
;; - Only use State V2 async APIs (async-value, async-update!, etc.)
;; - Do NOT mix synchronous and asynchronous state access
;; - Requires ForSt or async-enabled state backend
