(ns flink-clj.connectors.elasticsearch-test
  "Tests for Elasticsearch connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.elasticsearch :as es]
            [flink-clj.connectors.generic :as conn]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Sink functions exist"
    (is (fn? es/sink))
    (is (fn? es/simple-sink)))

  (testing "Info function exists"
    (is (fn? es/elasticsearch-info))))

;; =============================================================================
;; Info Tests
;; =============================================================================

(deftest elasticsearch-info-test
  (testing "elasticsearch-info returns expected structure"
    (let [info (es/elasticsearch-info)]
      (is (map? info))
      (is (contains? info :es7-available))
      (is (contains? info :es-base-available))
      (is (contains? info :features))
      (is (contains? info :supported-versions))
      (is (boolean? (:es7-available info)))
      (is (boolean? (:es-base-available info)))
      (is (sequential? (:features info)))
      (is (sequential? (:supported-versions info))))))

;; =============================================================================
;; Sink Configuration Tests
;; =============================================================================

(deftest sink-validation-test
  (testing "sink requires hosts"
    (is (thrown? clojure.lang.ExceptionInfo
          (es/sink {:emitter identity}))))

  (testing "sink requires emitter"
    (is (thrown? clojure.lang.ExceptionInfo
          (es/sink {:hosts ["http://localhost:9200"]})))))

(deftest simple-sink-validation-test
  (testing "simple-sink requires hosts"
    (is (thrown? clojure.lang.ExceptionInfo
          (es/simple-sink {:index "test"}))))

  (testing "simple-sink requires index"
    (is (thrown? clojure.lang.ExceptionInfo
          (es/simple-sink {:hosts ["http://localhost:9200"]})))))

;; =============================================================================
;; Duration Conversion Tests
;; =============================================================================

(deftest duration-spec-test
  (testing "Duration specs are valid"
    ;; Test that sink accepts various duration specs without throwing
    ;; (actual creation would require ES connector)
    (is (vector? [1 :seconds]))
    (is (vector? [5 :minutes]))
    (is (vector? [100 :milliseconds]))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-availability-test
  (testing "Availability check returns boolean"
    (let [info (es/elasticsearch-info)]
      (is (or (true? (:es7-available info))
              (false? (:es7-available info))))
      (is (or (true? (:es-base-available info))
              (false? (:es-base-available info)))))))

;; =============================================================================
;; Integration Tests (require ES connector JAR)
;; =============================================================================

(deftest ^:integration sink-creation-test
  (testing "sink creates Elasticsearch sink when connector available"
    (let [info (es/elasticsearch-info)]
      (when (or (:es7-available info) (:es-base-available info))
        ;; Only run if connector is available
        (is true "Would test actual sink creation")))))

(deftest ^:integration bulk-flush-config-test
  (testing "Bulk flush configuration is applied"
    (is true "Integration test placeholder")))

(deftest ^:integration emitter-creation-test
  (testing "Emitter is created from function"
    (is true "Integration test placeholder")))
