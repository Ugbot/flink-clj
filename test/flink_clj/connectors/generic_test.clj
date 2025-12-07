(ns flink-clj.connectors.generic-test
  "Tests for generic connector wrapper."
  (:require [clojure.test :refer :all]
            [flink-clj.connectors.generic :as conn])
  (:import [java.util Properties]))

;; =============================================================================
;; API Structure Tests
;; =============================================================================

(deftest api-functions-exist-test
  (testing "Builder creation functions exist"
    (is (fn? conn/source-builder))
    (is (fn? conn/sink-builder)))

  (testing "Builder operation functions exist"
    (is (fn? conn/invoke-static))
    (is (fn? conn/invoke))
    (is (fn? conn/set-prop))
    (is (fn? conn/configure))
    (is (fn? conn/with-properties))
    (is (fn? conn/build)))

  (testing "Schema functions exist"
    (is (fn? conn/string-schema))
    (is (fn? conn/json-schema))
    (is (fn? conn/edn-schema))
    (is (fn? conn/nippy-schema)))

  (testing "Utility functions exist"
    (is (fn? conn/connector-available?))
    (is (fn? conn/require-connector!))
    (is (fn? conn/list-available-connectors))
    (is (fn? conn/connector-info))))

;; =============================================================================
;; Builder Creation Tests
;; =============================================================================

(deftest source-builder-test
  (testing "source-builder creates proper record"
    (let [builder (conn/source-builder "org.example.MySource")]
      (is (= :source (:type builder)))
      (is (= "org.example.MySource" (:class-name builder)))
      (is (nil? (:builder builder)))
      (is (false? (:built? builder))))))

(deftest sink-builder-test
  (testing "sink-builder creates proper record"
    (let [builder (conn/sink-builder "org.example.MySink")]
      (is (= :sink (:type builder)))
      (is (= "org.example.MySink" (:class-name builder)))
      (is (nil? (:builder builder)))
      (is (false? (:built? builder))))))

;; =============================================================================
;; Type Coercion Tests
;; =============================================================================

(deftest coerce-arg-test
  (testing "String coercion"
    (let [coerce #'conn/coerce-arg]
      ;; Keyword to string
      (is (= "test" (coerce :test String)))
      ;; String passthrough
      (is (= "hello" (coerce "hello" String)))))

  (testing "Primitive coercion"
    (let [coerce #'conn/coerce-arg]
      (is (= (int 42) (coerce 42 Integer/TYPE)))
      (is (= (long 42) (coerce 42 Long/TYPE)))
      (is (= (double 3.14) (coerce 3.14 Double/TYPE)))))

  (testing "Properties coercion"
    (let [coerce #'conn/coerce-arg
          result (coerce {:foo "bar" :baz "qux"} Properties)]
      (is (instance? Properties result))
      (is (= "bar" (.getProperty result "foo")))
      (is (= "qux" (.getProperty result "baz")))))

  (testing "Duration coercion"
    (let [coerce #'conn/coerce-arg]
      (let [d (coerce [10 :seconds] java.time.Duration)]
        (is (instance? java.time.Duration d))
        (is (= 10 (.toSeconds d))))
      (let [d (coerce [5 :minutes] java.time.Duration)]
        (is (= 300 (.toSeconds d)))))))

;; =============================================================================
;; Serialization Schema Tests
;; =============================================================================

(deftest string-schema-test
  (testing "string-schema creates SimpleStringSchema"
    (let [schema (conn/string-schema)]
      (is (instance? org.apache.flink.api.common.serialization.SimpleStringSchema schema)))))

;; =============================================================================
;; Connector Availability Tests
;; =============================================================================

(deftest connector-available-test
  (testing "connector-available? returns false for non-existent class"
    (is (false? (conn/connector-available? "org.example.NonExistent"))))

  (testing "connector-available? returns true for existing class"
    (is (true? (conn/connector-available? "java.lang.String")))))

(deftest require-connector-test
  (testing "require-connector! throws for missing connector"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          #"Connector not available"
          (conn/require-connector! "org.example.NonExistent" "fake-connector")))))

;; =============================================================================
;; Connector Info Tests
;; =============================================================================

(deftest list-available-connectors-test
  (testing "list-available-connectors returns map"
    (let [connectors (conn/list-available-connectors)]
      (is (map? connectors))
      (is (contains? connectors "flink-connector-kafka"))
      (is (contains? connectors "flink-connector-jdbc")))))

(deftest connector-info-test
  (testing "connector-info returns proper structure"
    (let [info (conn/connector-info)]
      (is (map? info))
      (is (contains? info :description))
      (is (contains? info :features))
      (is (contains? info :available-connectors))
      (is (sequential? (:features info))))))

;; =============================================================================
;; Common Connectors Map Test
;; =============================================================================

(deftest common-connectors-test
  (testing "common-connectors has expected entries"
    (is (map? conn/common-connectors))
    (is (contains? conn/common-connectors "flink-connector-kafka"))
    (is (contains? conn/common-connectors "flink-connector-jdbc"))
    (is (contains? conn/common-connectors "flink-connector-elasticsearch7"))
    (is (contains? conn/common-connectors "flink-connector-pulsar"))))

;; =============================================================================
;; Builder Validation Tests
;; =============================================================================

(deftest invoke-without-builder-test
  (testing "invoke throws when no builder instance exists"
    (let [builder (conn/source-builder "org.example.MySource")]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"No builder instance"
            (conn/invoke builder "setFoo" ["bar"]))))))

(deftest set-prop-without-builder-test
  (testing "set-prop throws when no builder instance exists"
    (let [builder (conn/source-builder "org.example.MySource")]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"No builder instance"
            (conn/set-prop builder :foo "bar"))))))

(deftest build-without-builder-test
  (testing "build throws when no builder instance exists"
    (let [builder (conn/source-builder "org.example.MySource")]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"No builder instance"
            (conn/build builder))))))

;; =============================================================================
;; Integration Tests (require actual connectors)
;; =============================================================================

(deftest ^:integration kafka-builder-test
  (testing "Kafka source builder integration"
    (when (conn/connector-available? "org.apache.flink.connector.kafka.source.KafkaSource")
      (let [source (-> (conn/source-builder "org.apache.flink.connector.kafka.source.KafkaSource")
                       (conn/invoke-static "builder")
                       (conn/set-prop :bootstrap-servers "localhost:9092")
                       (conn/set-prop :topics ["test-topic"])
                       (conn/invoke "setValueOnlyDeserializer" [(conn/string-schema)])
                       (conn/build))]
        (is (some? source))))))

;; =============================================================================
;; Print Sink Test
;; =============================================================================

(deftest print-sink-test
  (testing "print-sink creates sink (version-aware)"
    (try
      (let [sink (conn/print-sink)]
        (is (some? sink)))
      (catch clojure.lang.ExceptionInfo e
        ;; If PrintSink not available, test passes (skip)
        (is (re-find #"PrintSink not available" (.getMessage e))))))

  (testing "print-sink with options (Flink 1.x)"
    ;; Only test with options on Flink 1.x where they're supported
    (when (try
            (Class/forName "org.apache.flink.streaming.api.functions.sink.PrintSinkFunction")
            true
            (catch ClassNotFoundException _ false))
      (let [sink (conn/print-sink {:prefix "TEST> "})]
        (is (some? sink)))
      (let [sink (conn/print-sink {:stderr? true})]
        (is (some? sink))))))
