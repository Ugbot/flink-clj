(ns flink-clj.iterate
  "Iterative stream processing for feedback loops.

  Iterations allow data to be fed back from downstream operators to upstream,
  enabling iterative algorithms like:
  - Incremental graph algorithms (PageRank, connected components)
  - Iterative ML training
  - Convergence-based computations

  IMPORTANT: Iterations are only available in Flink 1.x. They were removed in Flink 2.x.
  For Flink 2.x, consider using:
  - State-based iteration with timers
  - External systems for coordination
  - Batch processing for iterative algorithms

  Example (Flink 1.x only):
    (require '[flink-clj.iterate :as iter])

    ;; Fibonacci-like iteration
    (defn step-fn [x] (if (< x 100) (inc x) nil))
    (defn should-iterate? [x] (some? x))

    (let [input-stream (flink/from-collection env [1 2 3])
          iterative (iter/iterate input-stream 5000)  ; 5 second max wait
          step-result (stream/flink-map (:body iterative) #'step-fn)
          feedback (stream/flink-filter step-result #'should-iterate?)]
      (-> iterative
          (iter/close-with feedback)
          (stream/flink-filter #'some?)
          (sink/print)))"
  (:refer-clojure :exclude [iterate])
  (:require [flink-clj.version :as v]
            [flink-clj.impl.functions :as impl])
  (:import [org.apache.flink.streaming.api.datastream DataStream]))

;; =============================================================================
;; Version Check
;; =============================================================================

(defn- require-flink-1!
  "Ensure we're running on Flink 1.x for iterations."
  []
  (when (v/flink-2?)
    (throw (ex-info "Iterations are not available in Flink 2.x. They were removed from the DataStream API."
                    {:flink-version 2
                     :alternatives ["Use state-based iteration with timers"
                                    "Use external coordination systems"
                                    "Use Flink Batch API for iterative algorithms"]}))))

(defn- get-iterative-stream-class
  "Get the IterativeStream class (Flink 1.x only)."
  []
  (require-flink-1!)
  (Class/forName "org.apache.flink.streaming.api.datastream.IterativeStream"))

;; =============================================================================
;; Core Iteration API
;; =============================================================================

(defn iterate
  "Create an iterative stream from a DataStream.

  Returns a map containing:
    :iterative - The IterativeStream object
    :body      - The DataStream to apply iteration body operations to

  The body stream should be transformed and then fed back using close-with.

  max-wait-time-ms is the maximum time to wait for feedback (in milliseconds).
  If no feedback arrives within this time, the iteration terminates.

  Example:
    (let [{:keys [body] :as iter} (iterate stream 5000)
          result (-> body
                     (stream/flink-map #'process)
                     (stream/flink-filter #'continue?))]
      (close-with iter result))"
  [^DataStream stream max-wait-time-ms]
  (require-flink-1!)
  (let [iterative (.iterate stream (long max-wait-time-ms))]
    {:type :iterative-stream
     :iterative iterative
     :body iterative}))

(defn close-with
  "Close the iteration loop by specifying the feedback stream.

  The feedback stream is connected back to the head of the iteration.
  Elements in the feedback stream are fed back to the iteration body.
  Elements NOT in the feedback stream exit the iteration.

  Returns the output stream (elements that don't get fed back).

  Example:
    (let [iter (iterate input-stream 5000)
          processed (-> (:body iter)
                        (stream/flink-map #'step)
                        (stream/flink-filter #'not-converged?))]
      (close-with iter processed))"
  [iterative-map ^DataStream feedback-stream]
  (require-flink-1!)
  (let [iterative (:iterative iterative-map)]
    (.closeWith iterative feedback-stream)))

(defn close-with-output
  "Close the iteration loop with separate feedback and output streams.

  Some iterations need to emit output while also feeding back for more processing.
  This function handles that by:
  1. The feedback-stream goes back to iteration head
  2. The output-stream exits the iteration

  Note: Requires connecting both streams properly in Flink.
  The feedback and output should be disjoint (split from same source).

  Example:
    (let [iter (iterate input-stream 5000)
          processed (stream/flink-map (:body iter) #'step)
          ;; Split into converged (output) and not-converged (feedback)
          converged (stream/flink-filter processed #'converged?)
          not-converged (stream/flink-filter processed #'not-converged?)]
      ;; Feed back non-converged, output converged
      (close-with iter not-converged)
      converged)"
  [iterative-map ^DataStream feedback-stream]
  ;; Same as close-with - the output is whatever doesn't go to feedback
  (close-with iterative-map feedback-stream))

;; =============================================================================
;; Iteration Utilities
;; =============================================================================

(defn with-iteration
  "Higher-level iteration helper that handles the common pattern.

  Takes:
    stream          - Input DataStream
    max-wait-time   - Maximum wait time in ms
    body-fn         - Function that transforms the iteration body
    feedback-pred   - Predicate to determine what gets fed back

  Returns the output stream (elements where feedback-pred returns false).

  Example:
    (defn step [x] (update x :value inc))
    (defn should-continue? [x] (< (:value x) 100))

    (with-iteration input-stream 5000 #'step #'should-continue?)"
  [^DataStream stream max-wait-time-ms body-fn feedback-pred]
  (require-flink-1!)
  (let [{:keys [iterative body]} (iterate stream max-wait-time-ms)
        ;; Apply body transformation
        body-fn-wrapper (impl/make-map-function body-fn)
        transformed (.map ^DataStream body body-fn-wrapper)
        ;; Split into feedback and output
        feedback-wrapper (impl/make-filter-function feedback-pred)
        feedback (.filter transformed feedback-wrapper)]
    ;; Close the loop
    (.closeWith iterative feedback)
    ;; Return the transformed stream (caller can filter for output)
    transformed))

;; =============================================================================
;; Convergence Helpers
;; =============================================================================

(defn converge
  "Run iteration until convergence based on a delta threshold.

  Useful for algorithms that converge when the change is small enough.

  Takes:
    stream        - Input DataStream of elements with a :value key
    max-wait-time - Maximum wait time in ms
    step-fn       - Function to compute next iteration value
    delta-fn      - Function to compute delta between old and new values
    threshold     - Convergence threshold (stop when delta < threshold)

  Example:
    (defn gradient-step [x] (update x :params optimize))
    (defn compute-loss-delta [x] (Math/abs (- (:loss x) (:prev-loss x))))

    (converge input-stream 10000 #'gradient-step #'compute-loss-delta 0.001)"
  [^DataStream stream max-wait-time-ms step-fn delta-fn threshold]
  (require-flink-1!)
  ;; This is a more complex pattern - for now, document and leave to user
  (throw (ex-info "converge is not yet implemented. Use with-iteration for custom convergence logic."
                  {:suggestion "Use (with-iteration stream time step-fn #(> (delta-fn %) threshold))"})))

;; =============================================================================
;; Information
;; =============================================================================

(defn iterations-available?
  "Check if iterations are available in the current Flink version.

  Returns true for Flink 1.x, false for Flink 2.x."
  []
  (v/flink-1?))

(defn iteration-info
  "Get information about iteration support.

  Returns a map with:
    :available - Whether iterations are supported
    :flink-version - Current Flink major version
    :notes - Any relevant notes"
  []
  {:available (iterations-available?)
   :flink-version (v/flink-major-version)
   :notes (if (iterations-available?)
            "Full iteration support available"
            "Iterations removed in Flink 2.x. Consider state-based alternatives.")})
