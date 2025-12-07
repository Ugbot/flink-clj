(ns flink-clj.process-test
  "Tests for flink-clj.process namespace."
  (:require [clojure.test :refer :all]
            [flink-clj.process :as p]
            [flink-clj.impl.functions :as impl])
  (:import [org.apache.flink.util OutputTag]
           [org.apache.flink.api.common.typeinfo TypeInformation]))

;; =============================================================================
;; Output Tag Tests
;; =============================================================================

(deftest output-tag-test
  (testing "Create output tag with primitive type"
    (let [tag (p/output-tag "late-data" :string)]
      (is (instance? OutputTag tag))
      (is (= "late-data" (.getId tag)))))

  (testing "Create output tag with complex type"
    (let [tag (p/output-tag "errors" [:row :message :string :code :int])]
      (is (instance? OutputTag tag)))))

;; =============================================================================
;; Process Function Factory Tests
;; =============================================================================

;; Test handlers (must be top-level vars)
(defn test-process-handler [ctx element out]
  (.collect out element))

(defn test-on-timer-handler [ctx timestamp out]
  nil)

(defn test-open-handler [ctx]
  nil)

(defn test-close-handler []
  nil)

(deftest keyed-process-function-factory-test
  (testing "Create keyed process function with all handlers"
    (let [pf (impl/make-keyed-process-function
               {:process #'test-process-handler
                :on-timer #'test-on-timer-handler
                :open #'test-open-handler
                :close #'test-close-handler}
               nil)]
      (is (some? pf))))

  (testing "Create keyed process function with only process handler"
    (let [pf (impl/make-keyed-process-function
               {:process #'test-process-handler}
               nil)]
      (is (some? pf)))))

(deftest process-function-factory-test
  (testing "Create process function with all handlers"
    (let [pf (impl/make-process-function
               {:process #'test-process-handler
                :open #'test-open-handler
                :close #'test-close-handler})]
      (is (some? pf))))

  (testing "Create process function with only process handler"
    (let [pf (impl/make-process-function
               {:process #'test-process-handler})]
      (is (some? pf)))))

;; Note: Full integration tests for process functions require MiniCluster
;; and actual stream processing. These would be marked as :integration tests.
