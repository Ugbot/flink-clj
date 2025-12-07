(ns flink-clj.process-integration-test
  "Unit tests for process function construction and configuration."
  (:require [clojure.test :refer :all]
            [flink-clj.env :as env]
            [flink-clj.core :as flink]
            [flink-clj.keyed :as keyed]
            [flink-clj.process :as p])
  (:import [org.apache.flink.util OutputTag]
           [org.apache.flink.api.common.typeinfo Types]))

;; =============================================================================
;; Test Functions (must be top-level for serialization)
;; =============================================================================

(defn simple-process [ctx element out]
  (.collect out element))

(defn doubling-process [ctx element out]
  (.collect out (* element 2)))

(defn get-key [[k _v]] k)

;; =============================================================================
;; Output Tag Tests
;; =============================================================================

(deftest output-tag-test
  (testing "Create output tag with type spec"
    (let [tag (p/output-tag "side-output" :string)]
      (is (instance? OutputTag tag))
      (is (= "side-output" (.getId tag)))))

  (testing "Create output tags with various types"
    (is (instance? OutputTag (p/output-tag "long-output" :long)))
    (is (instance? OutputTag (p/output-tag "int-output" :int)))
    (is (instance? OutputTag (p/output-tag "double-output" :double)))
    (is (instance? OutputTag (p/output-tag "clj-output" :clojure)))))

;; =============================================================================
;; Process Function Construction Tests
;; =============================================================================

(deftest process-function-construction-test
  (testing "Create process function with handlers"
    (let [flink-env (env/create-env)
          stream (flink/from-collection flink-env [1 2 3] {:type :long})
          result (p/process stream {:process #'simple-process})]
      (is (some? result)))))

(deftest process-function-with-returns-test
  (testing "Create process function with return type"
    (let [flink-env (env/create-env)
          stream (flink/from-collection flink-env [1 2 3] {:type :long})
          result (p/process stream
                            {:process #'doubling-process}
                            {:returns :long})]
      (is (some? result)))))

;; =============================================================================
;; Keyed Process Function Construction Tests
;; =============================================================================

(deftest keyed-process-function-construction-test
  (testing "Create keyed process function"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1] ["b" 2]])
                           (keyed/key-by #'get-key {:key-type :string}))
          result (p/keyed-process keyed-stream {:process #'simple-process})]
      (is (some? result)))))

(deftest keyed-process-function-with-returns-test
  (testing "Create keyed process function with return type"
    (let [flink-env (env/create-env)
          keyed-stream (-> (flink/from-collection flink-env [["a" 1] ["b" 2]])
                           (keyed/key-by #'get-key {:key-type :string}))
          result (p/keyed-process keyed-stream
                                  {:process #'simple-process}
                                  {:returns :clojure})]
      (is (some? result)))))

;; =============================================================================
;; Timer Utility Tests
;; =============================================================================

;; Note: Timer utilities are tested by construction - actual execution
;; requires a running job with timer context

(deftest timer-utility-functions-exist-test
  (testing "Timer utility functions are defined"
    (is (fn? p/register-event-time-timer!))
    (is (fn? p/register-processing-time-timer!))
    (is (fn? p/delete-event-time-timer!))
    (is (fn? p/delete-processing-time-timer!))
    (is (fn? p/current-processing-time))
    (is (fn? p/current-watermark))))

;; =============================================================================
;; Context Helper Tests
;; =============================================================================

(deftest context-helper-functions-exist-test
  (testing "Context helper functions are defined"
    (is (fn? p/timestamp))
    (is (fn? p/timer-service))
    (is (fn? p/current-key))
    (is (fn? p/runtime-context))))

;; =============================================================================
;; Side Output Tests
;; =============================================================================

(deftest get-side-output-function-exists-test
  (testing "get-side-output function is defined"
    (is (fn? p/get-side-output))))

;; =============================================================================
;; Multiple Output Tags Test
;; =============================================================================

(deftest multiple-output-tags-test
  (testing "Multiple output tags can be created"
    (let [tag1 (p/output-tag "errors" :string)
          tag2 (p/output-tag "late-data" :long)
          tag3 (p/output-tag "debug" :clojure)]
      (is (not= (.getId tag1) (.getId tag2)))
      (is (not= (.getId tag2) (.getId tag3)))
      (is (= "errors" (.getId tag1)))
      (is (= "late-data" (.getId tag2)))
      (is (= "debug" (.getId tag3))))))
